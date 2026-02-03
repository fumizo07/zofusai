# 005
# routers/kb_parts/diary_core.py
from __future__ import annotations

import gzip
import re
import time
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
import urllib.request
import urllib.error

from sqlalchemy.orm import Session

from models import KBPerson, KBRegion, KBStore

# ---- optional: diary state table (if exists)
try:
    from models import KBDiaryState  # type: ignore
except Exception:
    KBDiaryState = None  # type: ignore


JST = timezone(timedelta(hours=9))

_DIARY_HTTP_TIMEOUT_SEC = 8
_DIARY_CACHE_TTL_SEC = 10 * 60  # 10分（取得結果のメモリキャッシュ）
_DIARY_MAX_BYTES = 1024 * 1024  # 1MB

# DBへの再チェック間隔（重い外部取得を間引く）
_DIARY_DB_RECHECK_INTERVAL_SEC = 60 * 30  # 30分

# ざっくり安全策（オープンプロキシ化を避ける）
_DIARY_ALLOWED_HOST_SUFFIXES = (
    "cityheaven.net",
    "dto.jp",
)

# diary_url -> (saved_monotonic, latest_ts_ms_or_None, err_str)
_DIARY_CACHE: Dict[str, Tuple[float, Optional[int], str]] = {}


# =========================
# DTO URL normalize
# =========================
_DTO_CANON_HOST = "www.dto.jp"
_DTO_HOSTS_EQUIV = {"dto.jp", "www.dto.jp", "s.dto.jp"}

# 追跡に不要なクエリは落としてURL揺れを減らす（必要が出たら足す）
_DTO_DROP_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
}


def normalize_dto_url(url: str) -> str:
    """
    dto.jp / s.dto.jp / www.dto.jp のURLを、同一性のため www.dto.jp（PC版）に寄せて正規化する。
    - scheme は https に統一
    - host は dto系なら www.dto.jp に統一
    - fragment は除去
    - 末尾スラッシュの揺れを軽く統一（/ 以外の末尾 / は落とす）
    - 追跡用の不要クエリは除去（必要なら拡張）
    失敗時は入力をtrimして返す（例外は投げない）
    """
    s = (url or "").strip()
    if not s:
        return ""

    # scheme無しが混ざった場合は https 扱いに寄せる
    if s.startswith("//"):
        s = "https:" + s
    elif not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", s):
        # "www.dto.jp/..." のようなケースは https を付ける
        if re.match(r"^(?:dto\.jp|www\.dto\.jp|s\.dto\.jp)/", s):
            s = "https://" + s

    try:
        u = urlparse(s)
    except Exception:
        return s

    scheme = "https"
    host = (u.hostname or "").lower().strip()
    if host in _DTO_HOSTS_EQUIV:
        host = _DTO_CANON_HOST

    # port は原則落とす（必要なら残すが、dto系は不要想定）
    netloc = host

    path = u.path or ""
    if path and path != "/" and path.endswith("/"):
        path = path[:-1]

    # query 正規化（順序保持しつつ不要キー除去）
    query = u.query or ""
    if query:
        try:
            pairs = parse_qsl(query, keep_blank_values=True)
            kept = []
            for k, v in pairs:
                kk = (k or "").strip()
                if not kk:
                    continue
                if kk in _DTO_DROP_QUERY_KEYS:
                    continue
                kept.append((kk, v))
            query = urlencode(kept, doseq=True)
        except Exception:
            # 失敗時はそのまま
            query = u.query or ""

    # fragment は除去
    frag = ""

    try:
        out = urlunparse((scheme, netloc, path, u.params or "", query, frag))
        return out
    except Exception:
        return s


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


def _http_get_text_with_status(url: str, timeout_sec: int = _DIARY_HTTP_TIMEOUT_SEC) -> Tuple[str, str]:
    """
    Returns: (html_text, err)
      - err == "" on success
      - err like "http_403", "timeout", "url_error", "read_error", etc on failure
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
            # ★ br を要求すると、Content-Encoding: br で返されて urllib 側で復号できず事故りやすい
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        },
        method="GET",
    )

    t0 = time.time()
    print(f"[diary] http_get start url={url}")

    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as res:
            status = getattr(res, "status", None)
            if status is None:
                try:
                    status = res.getcode()
                except Exception:
                    status = None

            raw = b""
            try:
                raw = res.read(_DIARY_MAX_BYTES + 1)
            except Exception:
                print(f"[diary] http_get fail(read) url={url} sec={time.time()-t0:.2f}")
                return "", "read_error"

            if len(raw) > _DIARY_MAX_BYTES:
                raw = raw[:_DIARY_MAX_BYTES]

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

            print(
                f"[diary] http_get ok status={status} bytes={len(raw)} enc={enc} ct={ct} sec={time.time()-t0:.2f}"
            )

            if status is not None and int(status) >= 400:
                return "", f"http_{int(status)}"

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
                return raw.decode(charset, errors="replace"), ""
            except Exception:
                return raw.decode("utf-8", errors="replace"), ""

    except urllib.error.HTTPError as e:
        code = getattr(e, "code", None)
        if code is not None:
            print(f"[diary] http_get http_error url={url} code={code} sec={time.time()-t0:.2f}")
            return "", f"http_{int(code)}"
        print(f"[diary] http_get http_error url={url} sec={time.time()-t0:.2f}")
        return "", "http_error"
    except TimeoutError:
        print(f"[diary] http_get timeout url={url} sec={time.time()-t0:.2f}")
        return "", "timeout"
    except urllib.error.URLError as e:
        print(f"[diary] http_get url_error url={url} err={repr(e)} sec={time.time()-t0:.2f}")
        return "", "url_error"
    except Exception as e:
        print(f"[diary] http_get fail url={url} err={repr(e)} sec={time.time()-t0:.2f}")
        return "", "http_fail"


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

    re_ymd_hm = re.compile(
        r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})(?:\s+|T)?(\d{1,2}):(\d{2})"
    )
    re_ymd = re.compile(r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})")
    re_jp_hm = re.compile(
        r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})"
    )
    re_jp = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
    re_md_hm = re.compile(r"(\d{1,2})[/\-](\d{1,2})(?:\s*|　)*(\d{1,2}):(\d{2})")
    re_md_jp_hm = re.compile(r"(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})")
    re_md = re.compile(r"(\d{1,2})[/\-](\d{1,2})")
    re_md_jp = re.compile(r"(\d{1,2})月\s*(\d{1,2})日")

    def upd(dt: Optional[datetime]) -> None:
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


def _cache_get(url: str) -> Tuple[Optional[int], str, bool]:
    """
    Returns: (ts, err, hit)
    """
    if not url:
        return None, "", False
    item = _DIARY_CACHE.get(url)
    if not item:
        return None, "", False
    saved_mono, ts, err = item
    try:
        if (time.monotonic() - float(saved_mono)) <= float(_DIARY_CACHE_TTL_SEC):
            return ts, err, True
    except Exception:
        pass
    return None, "", False


def _cache_set(url: str, ts: Optional[int], err: str) -> None:
    if not url:
        return
    try:
        _DIARY_CACHE[url] = (time.monotonic(), ts, (err or ""))
    except Exception:
        pass


def _fetch_latest_ts_via_urllib(diary_url: str) -> Tuple[Optional[int], str]:
    """
    urllibでHTML取得→日時抽出（extract_latest_diary_dt）でepoch(ms)化
    """
    html, http_err = _http_get_text_with_status(diary_url, timeout_sec=_DIARY_HTTP_TIMEOUT_SEC)
    if http_err:
        return None, http_err
    if not html:
        return None, "http_empty"

    dt = extract_latest_diary_dt(html)
    if dt is None:
        return None, "parse_no_datetime"

    ts = dt_to_epoch_ms(dt.astimezone(timezone.utc))
    if ts <= 0:
        return None, "epoch_failed"
    return ts, ""


def _fetch_latest_ts_via_playwright(diary_url: str) -> Tuple[Optional[int], str]:
    """
    Playwright版（routers/kb_parts/diary_fetcher_pw.py）を遅延importして呼ぶ。
    import失敗やPlaywright未導入でもサーバー起動が落ちないようにする。
    """
    try:
        from .diary_fetcher_pw import get_latest_diary_ts_ms as pw_get_latest_diary_ts_ms  # type: ignore
    except Exception as e:
        return None, f"pw_import_error:{type(e).__name__}"

    try:
        return pw_get_latest_diary_ts_ms(diary_url)
    except Exception as e:
        return None, f"pw_call_error:{type(e).__name__}"


# --- ここから：get_latest_diary_ts_ms を Playwright優先に（失敗時urllibフォールバック） ---
def get_latest_diary_ts_ms(person_url: str) -> Tuple[Optional[int], str]:
    """
    person_url から diary の最新投稿時刻(ms, UTC epoch)を返す。
    戻り: (latest_ts_ms_or_None, err_str)
      - 成功: (ts_ms, "")
      - 失敗: (None, "http_403" 等)
    """
    pu = (person_url or "").strip()
    if not pu:
        return None, "url_empty"

    base = pu[:-1] if pu.endswith("/") else pu
    diary_url = base + "/diary"

    # host制限（オープンプロキシ化防止）
    if not is_allowed_diary_url(diary_url):
        return None, "host_not_allowed"

    # メモリキャッシュ（Playwrightを毎回起動しない）
    ts_c, err_c, hit = _cache_get(diary_url)
    if hit:
        return ts_c, (err_c or "")

    t0 = time.time()
    print(f"[diary] fetch start url={diary_url}")

    # 1) Playwright優先
    ts_pw, err_pw = _fetch_latest_ts_via_playwright(diary_url)
    if ts_pw is not None and not err_pw:
        _cache_set(diary_url, ts_pw, "")
        print(f"[diary] fetch ok via=playwright url={diary_url} sec={time.time()-t0:.2f}")
        return ts_pw, ""

    # 2) フォールバック：urllib
    ts_u, err_u = _fetch_latest_ts_via_urllib(diary_url)
    if ts_u is not None and not err_u:
        _cache_set(diary_url, ts_u, "")
        print(f"[diary] fetch ok via=urllib url={diary_url} sec={time.time()-t0:.2f}")
        return ts_u, ""

    # 失敗：どちらのエラーを返すか（HTTP系を優先して返す）
    final_err = ""
    if err_pw and err_pw.startswith("http_"):
        final_err = err_pw
    elif err_u and err_u.startswith("http_"):
        final_err = err_u
    elif err_pw:
        final_err = err_pw
    else:
        final_err = err_u or "fetch_failed"

    _cache_set(diary_url, None, final_err)
    print(
        f"[diary] fetch fail url={diary_url} err_pw={err_pw!r} err_u={err_u!r} final={final_err!r} sec={time.time()-t0:.2f}"
    )
    return None, final_err
# --- ここまで：get_latest_diary_ts_ms ---


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


def get_diary_state_map(db: Session, person_ids: List[int]) -> Dict[int, object]:
    out: Dict[int, object] = {}
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


def get_or_create_diary_state(db: Session, state_map: Dict[int, object], person_id: int) -> Optional[object]:
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

    # ✅ デフォルトは False
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


# ---- 以下は互換のため残す
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


def apply_diary_push_monotonic(
    p: KBPerson,
    incoming_latest_ts_ms: Optional[int],
    checked_at: Optional[datetime],
    st: Optional[object] = None,
    raw_time: Optional[str] = None,
    client_id: Optional[str] = None,
    force: Optional[bool] = None,
    parser_version: Optional[str] = None,
) -> Tuple[bool, bool, Optional[int], Optional[int]]:
    """
    サーバ側の“強い正”:
      - latest_ts は単調増加（後退更新は禁止）
      - checked_at は常に更新（渡された場合）

    Returns: (latest_updated, checked_updated, stored_before, applied_after)
    """
    stored_before = get_person_diary_latest_ts(p, st=st)
    incoming_i = safe_int(incoming_latest_ts_ms)

    applied_after = stored_before
    latest_updated = False
    checked_updated = False

    if incoming_i is not None:
        if stored_before is None or incoming_i > stored_before:
            if set_person_diary_latest_ts(p, incoming_i, st=st):
                latest_updated = True
                applied_after = incoming_i

    if checked_at is not None:
        if set_person_diary_checked_at(p, checked_at, st=st):
            checked_updated = True

    # 最小限ログ（原因切り分け用）
    try:
        print(
            "[diary] push_apply "
            f"person_id={getattr(p, 'id', None)} "
            f"incoming={incoming_i} stored_before={stored_before} applied_after={applied_after} "
            f"raw_time={raw_time!r} client_id={client_id!r} force={force!r} parser={parser_version!r}"
        )
    except Exception:
        pass

    return latest_updated, checked_updated, stored_before, applied_after


def diary_db_recheck_interval_sec() -> int:
    return int(_DIARY_DB_RECHECK_INTERVAL_SEC)
