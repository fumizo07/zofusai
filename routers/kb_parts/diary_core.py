# 003
# routers/kb_parts/diary_core.py
from __future__ import annotations

import gzip
import re
import time
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse
import urllib.request
import urllib.error

from sqlalchemy.orm import Session

from models import KBPerson, KBRegion, KBStore

# ---- optional: diary state table (if exists)
try:
    from models import KBDiaryState  # type: ignore
except Exception:
    KBDiaryState = None  # type: ignore

# ---- optional: playwright fetcher (if exists)
try:
    # routers/kb_parts/diary_fetcher_pw.py
    from .diary_fetcher_pw import fetch_diary_html as _pw_fetch_diary_html  # type: ignore
except Exception:
    _pw_fetch_diary_html = None  # type: ignore


JST = timezone(timedelta(hours=9))

_DIARY_HTTP_TIMEOUT_SEC = 8
_DIARY_CACHE_TTL_SEC = 10 * 60  # 10分（HTTP取得結果のメモリキャッシュ）
_DIARY_MAX_BYTES = 1024 * 1024  # 1MB
_DIARY_UA = "Mozilla/5.0 (compatible; PersonalSearchKB/1.0; +https://example.invalid)"

# DBへの再チェック間隔（重い外部取得を間引く）
_DIARY_DB_RECHECK_INTERVAL_SEC = 60 * 30  # 30分

# ざっくり安全策（オープンプロキシ化を避ける）
_DIARY_ALLOWED_HOST_SUFFIXES = (
    "cityheaven.net",
    "dto.jp",
)

# url -> (saved_monotonic, latest_ts_ms_or_None, err_str)
_DIARY_CACHE: Dict[str, Tuple[float, Optional[int], str]] = {}


def parse_ids_csv(raw: str, limit: int = 30) -> List[int]:
    out: List[int] = []
    if not raw:
        return out
    for part in str(raw).split(","):
        s = (part or "").strip()
        if not s:
            continue
        if not re.fullmatch(r"\d+", s):
            continue
        try:
            v = int(s)
        except Exception:
            continue
        if v <= 0:
            continue
        out.append(v)
        if len(out) >= int(limit):
            break
    return out


def is_allowed_diary_url(url: str) -> bool:
    try:
        u = urlparse(url)
    except Exception:
        return False
    if u.scheme not in ("http", "https"):
        return False
    host = (u.hostname or "").lower().strip()
    if not host:
        return False
    for suf in _DIARY_ALLOWED_HOST_SUFFIXES:
        if host == suf or host.endswith("." + suf):
            return True
    return False


def _gzip_decompress_limited(raw: bytes, limit: int) -> bytes:
    if not raw:
        return b""
    try:
        with gzip.GzipFile(fileobj=BytesIO(raw), mode="rb") as gf:
            out = gf.read(int(limit) + 1)
            if len(out) > int(limit):
                return out[: int(limit)]
            return out
    except Exception:
        return raw


def _urllib_get_text_and_status(url: str, timeout_sec: int = _DIARY_HTTP_TIMEOUT_SEC) -> Tuple[str, int, str]:
    """
    Returns: (html_text, status_code, err)
      - err == "" on success
      - err examples: "http_403", "url_error", "exception", "too_large"
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        },
        method="GET",
    )

    t0 = time.time()
    print(f"[diary] urllib start url={url}")

    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as res:
            status = getattr(res, "status", None)
            if status is None:
                try:
                    status = res.getcode()
                except Exception:
                    status = 0
            status_i = int(status or 0)

            raw = res.read(_DIARY_MAX_BYTES + 1)
            if len(raw) > _DIARY_MAX_BYTES:
                raw = raw[:_DIARY_MAX_BYTES]
                print(f"[diary] urllib too_large url={url} bytes>{_DIARY_MAX_BYTES} sec={time.time()-t0:.2f}")
                return raw.decode("utf-8", errors="replace"), status_i, "too_large"

            enc = ""
            try:
                enc = (res.headers.get("Content-Encoding") or "").lower()
            except Exception:
                enc = ""

            ct = ""
            try:
                ct = res.headers.get("Content-Type") or ""
            except Exception:
                ct = ""

            if "gzip" in enc:
                raw = _gzip_decompress_limited(raw, _DIARY_MAX_BYTES)

            charset = "utf-8"
            try:
                m = re.search(r"charset=([a-zA-Z0-9_\-]+)", ct)
                if m:
                    charset = m.group(1)
            except Exception:
                charset = "utf-8"

            try:
                html = raw.decode(charset, errors="replace")
            except Exception:
                html = raw.decode("utf-8", errors="replace")

            print(
                f"[diary] urllib ok status={status_i} bytes={len(html)} ct={ct} enc={enc} sec={time.time()-t0:.2f}"
            )
            return html, status_i, ""

    except urllib.error.HTTPError as e:
        code = getattr(e, "code", None)
        status_i = int(code or 0)
        print(f"[diary] urllib http_error status={status_i} url={url} sec={time.time()-t0:.2f}")
        return "", status_i, f"http_{status_i}" if status_i else "http_error"

    except urllib.error.URLError as e:
        print(f"[diary] urllib url_error url={url} err={repr(e)} sec={time.time()-t0:.2f}")
        return "", 0, "url_error"

    except Exception as e:
        print(f"[diary] urllib exception url={url} err={repr(e)} sec={time.time()-t0:.2f}")
        return "", 0, "exception"


def _infer_year_for_md(month: int, day: int, now_jst: datetime) -> int:
    y = now_jst.year
    try:
        dt = datetime(y, month, day, 0, 0, tzinfo=JST)
    except Exception:
        return y
    if dt > now_jst + timedelta(days=30):
        return y - 1
    return y


def extract_latest_diary_dt(html: str) -> Optional[datetime]:
    if not html:
        return None

    scope = html
    idx = scope.find("写メ日記")
    if idx < 0:
        idx = scope.find("日記")
    if idx >= 0:
        scope = scope[idx : idx + 200000]
    else:
        scope = scope[:200000]

    now_jst = datetime.now(JST)
    best: Optional[datetime] = None

    re_ymd_hm = re.compile(r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})(?:\s+|T)?(\d{1,2}):(\d{2})")
    re_ymd = re.compile(r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})")
    re_jp_hm = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})")
    re_jp = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
    re_md_hm = re.compile(r"(\d{1,2})[/\-](\d{1,2})(?:\s*|　)*(\d{1,2}):(\d{2})")
    re_md_jp_hm = re.compile(r"(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})")
    re_md = re.compile(r"(\d{1,2})[/\-](\d{1,2})")
    re_md_jp = re.compile(r"(\d{1,2})月\s*(\d{1,2})日")

    def upd(dt: Optional[datetime]):
        nonlocal best
        if dt is None:
            return
        if best is None or dt > best:
            best = dt

    for m in re_ymd_hm.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            hh = int(m.group(4))
            mm = int(m.group(5))
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    for m in re_jp_hm.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            hh = int(m.group(4))
            mm = int(m.group(5))
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    for m in re_md_hm.finditer(scope):
        try:
            mo = int(m.group(1))
            d = int(m.group(2))
            hh = int(m.group(3))
            mm = int(m.group(4))
            y = _infer_year_for_md(mo, d, now_jst)
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    for m in re_md_jp_hm.finditer(scope):
        try:
            mo = int(m.group(1))
            d = int(m.group(2))
            hh = int(m.group(3))
            mm = int(m.group(4))
            y = _infer_year_for_md(mo, d, now_jst)
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    for m in re_ymd.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
        except Exception:
            continue

    for m in re_jp.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
        except Exception:
            continue

    if best is None:
        for m in re_md.finditer(scope):
            try:
                mo = int(m.group(1))
                d = int(m.group(2))
                y = _infer_year_for_md(mo, d, now_jst)
                upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
            except Exception:
                continue

        for m in re_md_jp.finditer(scope):
            try:
                mo = int(m.group(1))
                d = int(m.group(2))
                y = _infer_year_for_md(mo, d, now_jst)
                upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
            except Exception:
                continue

    return best


def dt_to_epoch_ms(dt: datetime) -> int:
    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _cache_get(url: str) -> Optional[Tuple[Optional[int], str]]:
    now_m = time.monotonic()
    v = _DIARY_CACHE.get(url)
    if not v:
        return None
    saved_m, ts, err = v
    if (now_m - saved_m) <= float(_DIARY_CACHE_TTL_SEC):
        return ts, err
    return None


def _cache_set(url: str, ts: Optional[int], err: str) -> None:
    _DIARY_CACHE[url] = (time.monotonic(), ts, err or "")


# --- ここから：get_latest_diary_ts_ms を丸ごと差し替え ---
def get_latest_diary_ts_ms(person_url: str) -> tuple[int | None, str]:
    """
    person_url から diary の最新投稿時刻(ms)を返す。
    失敗時は (None, "http_403" 等のエラー文字列) を返す。

    取得順:
      1) Playwright（あれば優先：403回避の可能性がある）
      2) urllib（軽量フォールバック）
    """
    pu = (person_url or "").strip()
    if not pu:
        return None, "url_empty"

    # 末尾スラッシュ除去して /diary を付ける（要件どおり）
    base = pu[:-1] if pu.endswith("/") else pu
    diary_url = base + "/diary"

    if not is_allowed_diary_url(diary_url):
        return None, "url_not_allowed"

    cached = _cache_get(diary_url)
    if cached is not None:
        ts, err = cached
        return ts, err

    # ---- 1) Playwright 優先 ----
    if _pw_fetch_diary_html is not None:
        t0 = time.time()
        print(f"[diary] pw start url={diary_url}")
        html, status, final_url, err = _pw_fetch_diary_html(diary_url)
        print(
            f"[diary] pw done url={diary_url} status={status} final={final_url} bytes={len(html)} err={err} sec={time.time()-t0:.2f}"
        )

        if err:
            # 403 は「弾かれた」ので、そのまま返す（urllibで改善しない可能性が高い）
            if err.startswith("http_403") or err == "http_403":
                _cache_set(diary_url, None, "http_403")
                return None, "http_403"
            # それ以外は urllib を試す
        else:
            dt = extract_latest_diary_dt(html)
            if dt is None:
                _cache_set(diary_url, None, "parse_failed")
                return None, "parse_failed"
            ts = dt_to_epoch_ms(dt)
            if ts <= 0:
                _cache_set(diary_url, None, "parse_failed")
                return None, "parse_failed"
            _cache_set(diary_url, ts, "")
            return ts, ""

    # ---- 2) urllib フォールバック ----
    html2, status2, err2 = _urllib_get_text_and_status(diary_url, timeout_sec=_DIARY_HTTP_TIMEOUT_SEC)

    if err2:
        # urllib 側が http_403 を返した場合も、ここまでで Playwright が無い/失敗なのでそのまま返す
        _cache_set(diary_url, None, err2)
        return None, err2

    # status が 4xx/5xx っぽい場合
    if status2 >= 400:
        e = f"http_{int(status2)}"
        _cache_set(diary_url, None, e)
        return None, e

    dt2 = extract_latest_diary_dt(html2)
    if dt2 is None:
        _cache_set(diary_url, None, "parse_failed")
        return None, "parse_failed"

    ts2 = dt_to_epoch_ms(dt2)
    if ts2 <= 0:
        _cache_set(diary_url, None, "parse_failed")
        return None, "parse_failed"

    _cache_set(diary_url, ts2, "")
    return ts2, ""
# --- ここまで：get_latest_diary_ts_ms を丸ごと差し替え ---


def build_diary_open_url_from_maps(
    person: KBPerson,
    store: Optional[KBStore],
    region: Optional[KBRegion],
) -> str:
    pu = ""
    if hasattr(person, "url"):
        pu = (getattr(person, "url", "") or "").strip()
    if pu:
        return pu

    parts = []
    if region and getattr(region, "name", None):
        parts.append(region.name)
    if store and getattr(store, "name", None):
        parts.append(store.name)
    if person and getattr(person, "name", None):
        parts.append(person.name)
    parts.append("写メ日記")
    q = " ".join([x for x in parts if x]).strip()
    return "https://www.google.com/search?" + urlencode({"q": q})


def bool_from_form(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in ("1", "true", "on", "yes")


# =========================
# diary state helpers（KBPerson列 or KBDiaryState）
# =========================
def diary_state_enabled() -> bool:
    return KBDiaryState is not None


def safe_bool(v: object) -> bool:
    """
    重要: bool("0") は True なので、それを避ける。
    """
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "on", "yes", "y", "t"):
            return True
        if s in ("0", "false", "off", "no", "n", "f", ""):
            return False
        return False
    try:
        return bool(v)
    except Exception:
        return False


def safe_int(v: object) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def get_diary_state_map(db: Session, person_ids: list[int]) -> dict[int, object]:
    out: dict[int, object] = {}
    if not diary_state_enabled() or not person_ids:
        return out
    try:
        rows = db.query(KBDiaryState).filter(KBDiaryState.person_id.in_(person_ids)).all()  # type: ignore
        for st in rows:
            pid = getattr(st, "person_id", None)
            if pid is None:
                continue
            out[int(pid)] = st
    except Exception:
        return {}
    return out


def get_or_create_diary_state(db: Session, state_map: dict[int, object], person_id: int) -> Optional[object]:
    if not diary_state_enabled():
        return None
    pid = int(person_id)
    st = state_map.get(pid)
    if st is not None:
        return st
    try:
        st = KBDiaryState(person_id=pid)  # type: ignore
    except Exception:
        try:
            st = KBDiaryState()  # type: ignore
            if hasattr(st, "person_id"):
                setattr(st, "person_id", pid)
        except Exception:
            return None
    try:
        db.add(st)  # type: ignore[arg-type]
    except Exception:
        pass
    state_map[pid] = st
    return st


def get_person_diary_track(p: KBPerson, st: Optional[object] = None) -> bool:
    """
    追跡ON/OFF の読み取り。
    models.py の定義に合わせて:
      - KBDiaryState.track_enabled
      - （互換）KBPerson.diary_track がある場合のみ読む
    """
    if st is not None:
        # ✅ 新定義
        if hasattr(st, "track_enabled"):
            return safe_bool(getattr(st, "track_enabled", False))
        # ✅ 旧互換
        if hasattr(st, "track"):
            return safe_bool(getattr(st, "track", False))

    if hasattr(p, "diary_track"):
        return safe_bool(getattr(p, "diary_track", False))

    # ✅ 「追跡ONがデフォルト」だと今回みたいに暴発するので、デフォルトは False にする
    return False


def set_person_diary_track(p: KBPerson, track: bool, st: Optional[object] = None) -> bool:
    """
    追跡ON/OFF の保存。
    models.py の定義に合わせて:
      - KBDiaryState.track_enabled に書く
      - （互換）KBPerson.diary_track がある場合はそちらに書く
    """
    changed = False

    if st is not None:
        # ✅ 新定義
        if hasattr(st, "track_enabled"):
            try:
                setattr(st, "track_enabled", bool(track))
                changed = True
            except Exception:
                pass
            return changed

        # ✅ 旧互換
        if hasattr(st, "track"):
            try:
                setattr(st, "track", bool(track))
                changed = True
            except Exception:
                pass
            return changed

    if hasattr(p, "diary_track"):
        try:
            setattr(p, "diary_track", bool(track))
            changed = True
        except Exception:
            pass

    return changed


# ---- 以下は互換のため残す（現状のmodels.pyと完全一致はしていませんが、壊しにくくする）
def get_person_diary_latest_ts(p: KBPerson, st: Optional[object] = None) -> Optional[int]:
    # state: latest_entry_at (datetime) があれば epoch ms に変換
    if st is not None:
        if hasattr(st, "latest_entry_at"):
            dt = getattr(st, "latest_entry_at", None)
            if isinstance(dt, datetime):
                return dt_to_epoch_ms(dt)
        for k in ("latest_ts_ms", "diary_latest_ts_ms", "latest_ts", "diary_latest_ts"):
            if hasattr(st, k):
                return safe_int(getattr(st, k, None))

    # person: diary_last_entry_at があれば epoch ms に変換
    if hasattr(p, "diary_last_entry_at"):
        dt = getattr(p, "diary_last_entry_at", None)
        if isinstance(dt, datetime):
            return dt_to_epoch_ms(dt)

    for k in ("diary_latest_ts_ms", "diary_latest_ts"):
        if hasattr(p, k):
            return safe_int(getattr(p, k, None))

    return None


def set_person_diary_latest_ts(p: KBPerson, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = safe_int(ts)
    changed = False

    # state: latest_entry_at があるなら datetime 化して入れる（ts->dt）
    if st is not None:
        if hasattr(st, "latest_entry_at"):
            try:
                if ts_i is None:
                    setattr(st, "latest_entry_at", None)
                else:
                    setattr(st, "latest_entry_at", datetime.fromtimestamp(ts_i / 1000, tz=JST))
                changed = True
            except Exception:
                pass
            return changed

        for k in ("latest_ts_ms", "diary_latest_ts_ms", "latest_ts", "diary_latest_ts"):
            if hasattr(st, k):
                try:
                    setattr(st, k, ts_i)
                    changed = True
                except Exception:
                    pass
                return changed

    # person: diary_last_entry_at があるなら datetime 化して入れる
    if hasattr(p, "diary_last_entry_at"):
        try:
            if ts_i is None:
                setattr(p, "diary_last_entry_at", None)
            else:
                setattr(p, "diary_last_entry_at", datetime.fromtimestamp(ts_i / 1000, tz=JST))
            changed = True
        except Exception:
            pass
        return changed

    for k in ("diary_latest_ts_ms", "diary_latest_ts"):
        if hasattr(p, k):
            try:
                setattr(p, k, ts_i)
                changed = True
            except Exception:
                pass
            return changed

    return False


def get_person_diary_seen_ts(p: KBPerson, st: Optional[object] = None) -> Optional[int]:
    if st is not None:
        if hasattr(st, "seen_at"):
            dt = getattr(st, "seen_at", None)
            if isinstance(dt, datetime):
                return dt_to_epoch_ms(dt)
        for k in ("seen_ts_ms", "diary_seen_ts_ms", "seen_ts", "diary_seen_ts"):
            if hasattr(st, k):
                return safe_int(getattr(st, k, None))

    if hasattr(p, "diary_seen_at"):
        dt = getattr(p, "diary_seen_at", None)
        if isinstance(dt, datetime):
            return dt_to_epoch_ms(dt)

    for k in ("diary_seen_ts_ms", "diary_seen_ts"):
        if hasattr(p, k):
            return safe_int(getattr(p, k, None))

    return None


def set_person_diary_seen_ts(p: KBPerson, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = safe_int(ts)
    changed = False

    if st is not None:
        if hasattr(st, "seen_at"):
            try:
                if ts_i is None:
                    setattr(st, "seen_at", None)
                else:
                    setattr(st, "seen_at", datetime.fromtimestamp(ts_i / 1000, tz=JST))
                changed = True
            except Exception:
                pass
            return changed

        for k in ("seen_ts_ms", "diary_seen_ts_ms", "seen_ts", "diary_seen_ts"):
            if hasattr(st, k):
                try:
                    setattr(st, k, ts_i)
                    changed = True
                except Exception:
                    pass
                return changed

    if hasattr(p, "diary_seen_at"):
        try:
            if ts_i is None:
                setattr(p, "diary_seen_at", None)
            else:
                setattr(p, "diary_seen_at", datetime.fromtimestamp(ts_i / 1000, tz=JST))
            changed = True
        except Exception:
            pass
        return changed

    for k in ("diary_seen_ts_ms", "diary_seen_ts"):
        if hasattr(p, k):
            try:
                setattr(p, k, ts_i)
                changed = True
            except Exception:
                pass
            return changed

    return False


def get_person_diary_checked_at(p: KBPerson, st: Optional[object] = None) -> Optional[datetime]:
    # 現行models.pyのstateには checked_at がないので、互換として person.diary_checked_at を使う
    if st is not None:
        for k in ("checked_at", "diary_checked_at", "last_checked_at"):
            if hasattr(st, k):
                try:
                    return getattr(st, k, None)
                except Exception:
                    return None
    if hasattr(p, "diary_checked_at"):
        try:
            return getattr(p, "diary_checked_at", None)
        except Exception:
            return None
    return None


def set_person_diary_checked_at(p: KBPerson, dt: Optional[datetime], st: Optional[object] = None) -> bool:
    # 現行models.pyのstateには checked_at がないので、互換として person.diary_checked_at を使う
    if st is not None:
        for k in ("checked_at", "diary_checked_at", "last_checked_at"):
            if hasattr(st, k):
                try:
                    setattr(st, k, dt)
                    return True
                except Exception:
                    return False
    if hasattr(p, "diary_checked_at"):
        try:
            setattr(p, "diary_checked_at", dt)
            return True
        except Exception:
            return False
    return False


def diary_db_recheck_interval_sec() -> int:
    return int(_DIARY_DB_RECHECK_INTERVAL_SEC)
