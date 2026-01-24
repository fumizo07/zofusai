# 027
# routers/kb_diary_core.py
import gzip
import re
import time
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Dict, Optional, Tuple
from urllib.parse import urlencode, urlparse
import urllib.request
import urllib.error

from sqlalchemy.orm import Session

from models import KBPerson, KBRegion, KBStore

# ---- optional: diary state table (if exists)
try:
    # 想定名: KBDiaryState（person_idで1行）
    from models import KBDiaryState  # type: ignore
except Exception:
    KBDiaryState = None  # type: ignore

JST = timezone(timedelta(hours=9))

_DIARY_HTTP_TIMEOUT_SEC = 8
_DIARY_CACHE_TTL_SEC = 10 * 60
_DIARY_MAX_BYTES = 1024 * 1024
_DIARY_UA = "Mozilla/5.0 (compatible; PersonalSearchKB/1.0; +https://example.invalid)"

_DIARY_DB_RECHECK_INTERVAL_SEC = 2 * 60 * 60

_DIARY_ALLOWED_HOST_SUFFIXES = (
    "cityheaven.net",
    "dto.jp",
)

# url -> (saved_monotonic, latest_ts_ms_or_None, err_str)
_DIARY_CACHE: Dict[str, Tuple[float, Optional[int], str]] = {}


def _is_allowed_diary_url(url: str) -> bool:
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


def _http_get_text(url: str, timeout_sec: int = _DIARY_HTTP_TIMEOUT_SEC) -> str:
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
            raw = _gzip_decompress_limited(raw, _DIARY_MAX_BYTES)

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


def _infer_year_for_md(month: int, day: int, now_jst: datetime) -> int:
    y = now_jst.year
    try:
        dt = datetime(y, month, day, 0, 0, tzinfo=JST)
    except Exception:
        return y
    if dt > now_jst + timedelta(days=30):
        return y - 1
    return y


def _extract_latest_diary_dt(html: str) -> Optional[datetime]:
    if not html:
        return None

    scope = html
    idx = scope.find("写メ日記")
    if idx < 0:
        idx = scope.find("日記")
    if idx >= 0:
        scope = scope[idx: idx + 200000]
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


def _dt_to_epoch_ms(dt: datetime) -> int:
    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _get_latest_diary_ts_ms(person_url: str) -> Tuple[Optional[int], str]:
    url = (person_url or "").strip()
    if not url:
        return None, "url_empty"
    if not _is_allowed_diary_url(url):
        return None, "unsupported_host"

    now_m = time.monotonic()
    cached = _DIARY_CACHE.get(url)
    if cached:
        saved_m, latest_ts, err = cached
        if now_m - saved_m <= _DIARY_CACHE_TTL_SEC:
            return latest_ts, err

    try:
        html = _http_get_text(url, timeout_sec=_DIARY_HTTP_TIMEOUT_SEC)
        dt = _extract_latest_diary_dt(html)
        if not dt:
            _DIARY_CACHE[url] = (now_m, None, "no_datetime_found")
            return None, "no_datetime_found"
        ts = _dt_to_epoch_ms(dt)
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


def _build_google_search_url(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return ""
    return "https://www.google.com/search?" + urlencode({"q": q})


def _build_google_site_search_url(domain: str, query: str) -> str:
    d = (domain or "").strip()
    q = (query or "").strip()
    if not d or not q:
        return ""
    return _build_google_search_url(f"site:{d} {q}")


def _build_diary_open_url_from_maps(
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
    return _build_google_search_url(q)


def _bool_from_form(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in ("1", "true", "on", "yes")


# =========================
# diary state helpers（KBPerson列 or KBDiaryState）
# =========================
def _diary_state_enabled() -> bool:
    return KBDiaryState is not None


def _safe_bool(v) -> bool:
    try:
        return bool(v)
    except Exception:
        return False


def _safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _get_diary_state_map(db: Session, person_ids: list[int]) -> dict[int, object]:
    out: dict[int, object] = {}
    if not _diary_state_enabled() or not person_ids:
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


def _get_or_create_diary_state(db: Session, state_map: dict[int, object], person_id: int) -> Optional[object]:
    if not _diary_state_enabled():
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


def _get_person_diary_track(p: KBPerson, st: Optional[object] = None) -> bool:
    if st is not None and hasattr(st, "track"):
        return _safe_bool(getattr(st, "track", False))
    if hasattr(p, "diary_track"):
        return _safe_bool(getattr(p, "diary_track", False))
    return True


def _set_person_diary_track(p: KBPerson, track: bool, st: Optional[object] = None) -> bool:
    changed = False
    if st is not None and hasattr(st, "track"):
        try:
            setattr(st, "track", bool(track))
            changed = True
        except Exception:
            pass
    elif hasattr(p, "diary_track"):
        try:
            setattr(p, "diary_track", bool(track))
            changed = True
        except Exception:
            pass
    return changed


def _get_person_diary_latest_ts(p: KBPerson, st: Optional[object] = None) -> Optional[int]:
    if st is not None:
        for k in ("latest_ts_ms", "diary_latest_ts_ms", "latest_ts", "diary_latest_ts"):
            if hasattr(st, k):
                return _safe_int(getattr(st, k, None))
    for k in ("diary_latest_ts_ms", "diary_latest_ts"):
        if hasattr(p, k):
            return _safe_int(getattr(p, k, None))
    return None


def _set_person_diary_latest_ts(p: KBPerson, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = _safe_int(ts)
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
        if hasattr(p, k):
            try:
                setattr(p, k, ts_i)
                changed = True
            except Exception:
                pass
            break
    return changed


def _get_person_diary_seen_ts(p: KBPerson, st: Optional[object] = None) -> Optional[int]:
    if st is not None:
        for k in ("seen_ts_ms", "diary_seen_ts_ms", "seen_ts", "diary_seen_ts"):
            if hasattr(st, k):
                return _safe_int(getattr(st, k, None))
    for k in ("diary_seen_ts_ms", "diary_seen_ts"):
        if hasattr(p, k):
            return _safe_int(getattr(p, k, None))
    return None


def _set_person_diary_seen_ts(p: KBPerson, ts: Optional[int], st: Optional[object] = None) -> bool:
    ts_i = _safe_int(ts)
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
        if hasattr(p, k):
            try:
                setattr(p, k, ts_i)
                changed = True
            except Exception:
                pass
            break
    return changed


def _get_person_diary_checked_at(p: KBPerson, st: Optional[object] = None) -> Optional[datetime]:
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


def _set_person_diary_checked_at(p: KBPerson, dt: Optional[datetime], st: Optional[object] = None) -> bool:
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
