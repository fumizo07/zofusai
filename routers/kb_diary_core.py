# 001
# routers/kb_diary_core.py
from __future__ import annotations

import gzip
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

try:
    # SQLAlchemyがある前提（型だけ使う）
    from sqlalchemy.orm import Session  # type: ignore
except Exception:  # pragma: no cover
    Session = object  # type: ignore

# =========================
# 写メ日記 NEWチェック（DB版）Core
# - 追跡対象: track == True のみ
# - latest_ts はサーバが取得し、DBに保存（TTL/intervalで再取得を抑制）
# - seen_ts はDBに保存（クリックで既読化）
# - 仕様B: seen_ts が未設定(None)の場合、初回取得時に seen_ts=latest_ts で初期化し NEWを出さない
#
# ★互換方針：
# - (A) KBPersonに diary_* 列があるならそれを使う
# - (B) models に KBDiaryState があるならそれを使う（person_id 1:1）
# - 両方ある場合は「KBDiaryState優先」
# =========================

JST = timezone(timedelta(hours=9))

_DIARY_HTTP_TIMEOUT_SEC = 8
_DIARY_CACHE_TTL_SEC = 10 * 60  # 10分（HTTP取得結果のメモリキャッシュ）
_DIARY_MAX_BYTES = 1024 * 1024  # 1MB
_DIARY_UA = "Mozilla/5.0 (compatible; PersonalSearchKB/1.0; +https://example.invalid)"

# DBへの再チェック間隔（ここが「検索のたびに重い」を潰す本体）
_DIARY_DB_RECHECK_INTERVAL_SEC = 2 * 60 * 60  # 2時間

# ざっくり安全策（オープンプロキシ化を避ける）
_DIARY_ALLOWED_HOST_SUFFIXES = (
    "cityheaven.net",
    "dto.jp",
)

# url -> (saved_monotonic, latest_ts_ms_or_None, err_str)
_DIARY_CACHE: Dict[str, Tuple[float, Optional[int], str]] = {}


# ============================================================
# 低レベル共通
# ============================================================
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


def gzip_decompress_limited(raw: bytes, limit: int) -> bytes:
    """
    gzip 展開時の“膨張”に上限をかける（zip爆弾対策）。
    - 展開後 limit bytes までしか読まない
    """
    if not raw:
        return b""
    try:
        with gzip.GzipFile(fileobj=BytesIO(raw), mode="rb") as gf:
            out = gf.read(int(limit) + 1)
            if len(out) > int(limit):
                return out[: int(limit)]
            return out
    except Exception:
        # 壊れてても無理しない
        return raw


def http_get_text(url: str, timeout_sec: int = _DIARY_HTTP_TIMEOUT_SEC) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _DIARY_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as res:
        raw = res.read(_DIARY_MAX_BYTES + 1)
        if len(raw) > _DIARY_MAX_BYTES:
            raw = raw[:_DIARY_MAX_BYTES]

        enc = ""
        try:
            enc = (res.headers.get("Content-Encoding") or "").lower()
        except Exception:
            enc = ""

        if "gzip" in enc:
            raw = gzip_decompress_limited(raw, _DIARY_MAX_BYTES)

        charset = "utf-8"
        try:
            ct = res.headers.get("Content-Type") or ""
            m = re.search(r"charset=([a-zA-Z0-9_\-]+)", ct)
            if m:
                charset = m.group(1)
        except Exception:
            charset = "utf-8"

        try:
            return raw.decode(charset, errors="replace")
        except Exception:
            return raw.decode("utf-8", errors="replace")


def infer_year_for_md(month: int, day: int, now_jst: datetime) -> int:
    """
    年が無い "M/D" や "M月D日" を現在年で補完しつつ、
    未来になりすぎる場合は前年扱いに寄せる。
    """
    y = now_jst.year
    try:
        dt = datetime(y, month, day, 0, 0, tzinfo=JST)
    except Exception:
        return y
    # 未来に30日以上飛ぶのは前年の可能性が高い
    if dt > now_jst + timedelta(days=30):
        return y - 1
    return y


def extract_latest_diary_dt(html: str) -> Optional[datetime]:
    if not html:
        return None

    # 「写メ日記」周辺を優先して誤爆を減らす（無ければ先頭から）
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

    # 1) YYYY/MM/DD HH:MM or YYYY-MM-DD HH:MM or YYYY.MM.DD HH:MM
    re_ymd_hm = re.compile(
        r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})(?:\s+|T)?(\d{1,2}):(\d{2})"
    )
    # 2) YYYY/MM/DD (or - or .)
    re_ymd = re.compile(r"(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})")
    # 3) YYYY年M月D日 HH:MM
    re_jp_hm = re.compile(
        r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})"
    )
    # 3b) YYYY年M月D日（時刻なし）
    re_jp = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
    # 4) M/D HH:MM
    re_md_hm = re.compile(r"(\d{1,2})[/\-](\d{1,2})(?:\s*|　)*(\d{1,2}):(\d{2})")
    # 4b) M月D日 HH:MM（年なし）
    re_md_jp_hm = re.compile(r"(\d{1,2})月\s*(\d{1,2})日(?:\s*|　)*(\d{1,2}):(\d{2})")
    # 5) M/D
    re_md = re.compile(r"(\d{1,2})[/\-](\d{1,2})")
    # 5b) M月D日（年なし）
    re_md_jp = re.compile(r"(\d{1,2})月\s*(\d{1,2})日")

    def upd(dt: Optional[datetime]) -> None:
        nonlocal best
        if dt is None:
            return
        if best is None or dt > best:
            best = dt

    # 1)
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

    # 3)
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

    # 4)
    for m in re_md_hm.finditer(scope):
        try:
            mo = int(m.group(1))
            d = int(m.group(2))
            hh = int(m.group(3))
            mm = int(m.group(4))
            y = infer_year_for_md(mo, d, now_jst)
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    # 4b)
    for m in re_md_jp_hm.finditer(scope):
        try:
            mo = int(m.group(1))
            d = int(m.group(2))
            hh = int(m.group(3))
            mm = int(m.group(4))
            y = infer_year_for_md(mo, d, now_jst)
            upd(datetime(y, mo, d, hh, mm, tzinfo=JST))
        except Exception:
            continue

    # 2)
    for m in re_ymd.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
        except Exception:
            continue

    # 3b)（時刻なし）
    for m in re_jp.finditer(scope):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            d = int(m.group(3))
            upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
        except Exception:
            continue

    # 5/5b は誤爆しやすいので、すでにbestがあるなら追加しない
    if best is None:
        for m in re_md.finditer(scope):
            try:
                mo = int(m.group(1))
                d = int(m.group(2))
                y = infer_year_for_md(mo, d, now_jst)
                upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
            except Exception:
                continue

        for m in re_md_jp.finditer(scope):
            try:
                mo = int(m.group(1))
                d = int(m.group(2))
                y = infer_year_for_md(mo, d, now_jst)
                upd(datetime(y, mo, d, 0, 0, tzinfo=JST))
            except Exception:
                continue

    return best


def dt_to_epoch_ms(dt: datetime) -> int:
    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def get_latest_diary_ts_ms(person_url: str) -> Tuple[Optional[int], str]:
    """
    戻り: (latest_ts_ms or None, err)
    - None の場合は取得不能
    """
    url = (person_url or "").strip()
    if not url:
        return None, "url_empty"
    if not is_allowed_diary_url(url):
        return None, "unsupported_host"

    now_m = time.monotonic()
    cached = _DIARY_CACHE.get(url)
    if cached:
        saved_m, latest_ts, err = cached
        if now_m - saved_m <= _DIARY_CACHE_TTL_SEC:
            return latest_ts, err

    try:
        html = http_get_text(url, timeout_sec=_DIARY_HTTP_TIMEOUT_SEC)
        dt = extract_latest_diary_dt(html)
        if not dt:
            _DIARY_CACHE[url] = (now_m, None, "no_datetime_found")
            return None, "no_datetime_found"
        ts = dt_to_epoch_ms(dt)
        if ts <= 0:
            _DIARY_CACHE[url] = (now_m, None, "bad_datetime")
            return None, "bad_datetime"
        _DIARY_CACHE[url] = (now_m, ts, "")
        return ts, ""
    except urllib.error.HTTPError as e:
        msg = f"http_error_{getattr(e, 'code', '')}"
        _DIARY_CACHE[url] = (now_m, None, msg)
        return None, msg
    except Exception:
        _DIARY_CACHE[url] = (now_m, None, "fetch_error")
        return None, "fetch_error"


# ============================================================
# URL生成（フォールバック用）
# ============================================================
def build_google_search_url(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return ""
    return "https://www.google.com/search?" + urlencode({"q": q})


def build_google_site_search_url(domain: str, query: str) -> str:
    d = (domain or "").strip()
    q = (query or "").strip()
    if not d or not q:
        return ""
    return build_google_search_url(f"site:{d} {q}")


def build_diary_open_url_from_maps(person, store=None, region=None) -> str:
    """
    NEWクリック時に飛ばす先（DB問い合わせしない版）。
    - person.url があればそれを優先
    - 無ければ Google 検索（地域/店/名前 + 写メ日記）で代替
    """
    pu = ""
    try:
        if hasattr(person, "url"):
            pu = (getattr(person, "url", "") or "").strip()
    except Exception:
        pu = ""

    if pu:
        return pu

    parts: List[str] = []
    try:
        if region is not None and getattr(region, "name", None):
            parts.append(str(region.name))
    except Exception:
        pass
    try:
        if store is not None and getattr(store, "name", None):
            parts.append(str(store.name))
    except Exception:
        pass
    try:
        if person is not None and getattr(person, "name", None):
            parts.append(str(person.name))
    except Exception:
        pass

    parts.append("写メ日記")
    q = " ".join([x for x in parts if x]).strip()
    return build_google_search_url(q)


def bool_from_form(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in ("1", "true", "on", "yes")


# ============================================================
# diary state helpers（KBPerson列 or KBDiaryState）
# ============================================================
def safe_bool(v) -> bool:
    try:
        return bool(v)
    except Exception:
        return False


def safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def diary_state_enabled(KBDiaryStateModel) -> bool:
    return KBDiaryStateModel is not None


def get_diary_state_map(db: Session, KBDiaryStateModel, person_ids: List[int]) -> Dict[int, object]:
    """
    person_id -> state_obj
    """
    out: Dict[int, object] = {}
    if not diary_state_enabled(KBDiaryStateModel) or not person_ids:
        return out
    try:
        rows = db.query(KBDiaryStateModel).filter(KBDiaryStateModel.person_id.in_(person_ids)).all()  # type: ignore
        for st in rows:
            pid = getattr(st, "person_id", None)
            if pid is None:
                continue
            out[int(pid)] = st
    except Exception:
        return {}
    return out


def get_or_create_diary_state(
    db: Session,
    KBDiaryStateModel,
    state_map: Dict[int, object],
    person_id: int,
) -> Optional[object]:
    if not diary_state_enabled(KBDiaryStateModel):
        return None
    pid = int(person_id)
    st = state_map.get(pid)
    if st is not None:
        return st
    try:
        st = KBDiaryStateModel(person_id=pid)  # type: ignore
    except Exception:
        try:
            st = KBDiaryStateModel()  # type: ignore
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


def get_person_diary_track(person, st: Optional[object] = None) -> bool:
    # state優先
    if st is not None and hasattr(st, "track"):
        return safe_bool(getattr(st, "track", False))
    if hasattr(person, "diary_track"):
        return safe_bool(getattr(person, "diary_track", False))
    # 列がまだ無い間は「従来挙動」を保つ（壊さない）
    return True


def set_person_diary_track(person, track: bool, st: Optional[object] = None) -> bool:
    changed = False
    if st is not None and hasattr(st, "track"):
        try:
            setattr(st, "track", bool(track))
            changed = True
        except Exception:
            pass
    elif hasattr(person, "diary_track"):
        try:
            setattr(person, "diary_track", bool(track))
            changed = True
        except Exception:
            pass
    return changed


def get_person_diary_latest_ts(person, st: Optional[object] = None) -> Optional[int]:
    if st is not None:
        for k in ("latest_ts_ms", "diary_latest_ts_ms", "latest_ts", "diary_latest_ts"):
            if hasattr(st, k):
                return safe_int(getattr(st, k, None))
    for k in ("diary_latest_ts_ms", "diary_latest_ts"):
        if hasattr(person, k):
            return safe_int(getattr(person, k, None))
    return None


def set_person_diary_latest_ts(person, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = safe_int(ts)
    changed = False

    if st is not None:
        for k in ("latest_ts_ms", "diary_latest_ts_ms", "latest_ts", "diary_latest_ts"):
            if hasattr(st, k):
                try:
                    setattr(st, k, ts_i)
                    changed = True
                except Exception:
                    pass
                break
        if changed:
            return True

    for k in ("diary_latest_ts_ms", "diary_latest_ts"):
        if hasattr(person, k):
            try:
                setattr(person, k, ts_i)
                changed = True
            except Exception:
                pass
            break
    return changed


def get_person_diary_seen_ts(person, st: Optional[object] = None) -> Optional[int]:
    if st is not None:
        for k in ("seen_ts_ms", "diary_seen_ts_ms", "seen_ts", "diary_seen_ts"):
            if hasattr(st, k):
                return safe_int(getattr(st, k, None))
    for k in ("diary_seen_ts_ms", "diary_seen_ts"):
        if hasattr(person, k):
            return safe_int(getattr(person, k, None))
    return None


def set_person_diary_seen_ts(person, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = safe_int(ts)
    changed = False

    if st is not None:
        for k in ("seen_ts_ms", "diary_seen_ts_ms", "seen_ts", "diary_seen_ts"):
            if hasattr(st, k):
                try:
                    setattr(st, k, ts_i)
                    changed = True
                except Exception:
                    pass
                break
        if changed:
            return True

    for k in ("diary_seen_ts_ms", "diary_seen_ts"):
        if hasattr(person, k):
            try:
                setattr(person, k, ts_i)
                changed = True
            except Exception:
                pass
            break
    return changed


def get_person_diary_checked_at(person, st: Optional[object] = None) -> Optional[datetime]:
    if st is not None:
        for k in ("checked_at", "diary_checked_at", "last_checked_at"):
            if hasattr(st, k):
                try:
                    return getattr(st, k, None)
                except Exception:
                    return None
    if hasattr(person, "diary_checked_at"):
        try:
            return getattr(person, "diary_checked_at", None)
        except Exception:
            return None
    return None


def set_person_diary_checked_at(person, dt: Optional[datetime], st: Optional[object] = None) -> bool:
    if st is not None:
        for k in ("checked_at", "diary_checked_at", "last_checked_at"):
            if hasattr(st, k):
                try:
                    setattr(st, k, dt)
                    return True
                except Exception:
                    return False
    if hasattr(person, "diary_checked_at"):
        try:
            setattr(person, "diary_checked_at", dt)
            return True
        except Exception:
            return False
    return False


# ============================================================
# re-check 判定（ルーター側から使う用）
# ============================================================
def should_recheck_db(checked_at: Optional[datetime], now: Optional[datetime] = None) -> bool:
    """
    DB再チェックの要否。
    - checked_at が無い → チェックする
    - 2時間以内 → チェックしない
    """
    if checked_at is None:
        return True
    now_dt = now or datetime.now(JST)
    try:
        age = (now_dt - checked_at).total_seconds()
    except Exception:
        return True
    return age >= float(_DIARY_DB_RECHECK_INTERVAL_SEC)
