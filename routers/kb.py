# 026
# routers/kb.py
import json
import re
import unicodedata
import time
import gzip
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse
import urllib.request
import urllib.error

from fastapi import APIRouter, Depends, Form, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from sqlalchemy import and_, desc, exists, func, or_, text
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit, KBPriceTemplate

# ---- optional: diary state table (if exists)
try:
    # 想定名: KBDiaryState（person_idで1行）
    from models import KBDiaryState  # type: ignore
except Exception:
    KBDiaryState = None  # type: ignore

router = APIRouter()

# =========================
# 写メ日記 NEWチェック（DB版）
# - 追跡対象: track == True のみ
# - latest_ts はサーバが取得し、DBに保存（TTL/intervalで再取得を抑制）
# - seen_ts はDBに保存（クリックで既読化）
# - 仕様B: seen_ts が未設定(None)の場合、初回取得時に seen_ts=latest_ts で初期化し NEWを出さない
#
# ★互換方針（#026）：
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


def _parse_ids_csv(raw: str, limit: int = 30) -> List[int]:
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


def _extract_latest_diary_dt(html: str) -> Optional[datetime]:
    if not html:
        return None

    # 「写メ日記」周辺を優先して誤爆を減らす（無ければ先頭から）
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

    def upd(dt: Optional[datetime]):
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
            y = _infer_year_for_md(mo, d, now_jst)
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
            y = _infer_year_for_md(mo, d, now_jst)
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
    """
    戻り: (latest_ts_ms or None, err)
    - None の場合は取得不能
    """
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
    """
    NEWクリック時に飛ばす先（DB問い合わせしない版）。
    - person.url があればそれを優先
    - 無ければ Google 検索（地域/店/名前 + 写メ日記）で代替
    """
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
    """
    person_id -> state_obj
    """
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
        # person_id必須想定
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
    # state優先
    if st is not None and hasattr(st, "track"):
        return _safe_bool(getattr(st, "track", False))
    if hasattr(p, "diary_track"):
        return _safe_bool(getattr(p, "diary_track", False))
    # 列がまだ無い間は「従来挙動」を保つ（壊さない）
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


@router.get("/kb/api/diary_latest")
def kb_api_diary_latest(
    ids: str = Query(""),
    db: Session = Depends(get_db),
):
    """
    フロントが「最新日記の日時 + NEW判定」を取得するAPI（DB版）。
    - 追跡OFFはスキップ（tracked=False で返す）
    - DBの checked_at が新しければ外部取得を省略
    - 初回（seen_ts=None）は seen_ts=latest_ts に初期化し NEWを出さない（仕様B）
    """
    person_ids = _parse_ids_csv(ids, limit=30)
    if not person_ids:
        return JSONResponse({"ok": True, "items": []})

    persons = db.query(KBPerson).filter(KBPerson.id.in_(person_ids)).all()
    pmap = {int(getattr(p, "id", 0)): p for p in persons if p and getattr(p, "id", None)}

    # stateまとめ取り（ある場合）
    state_map = _get_diary_state_map(db, person_ids)

    # open_url 生成のために store/region をまとめて引く（N+1回避）
    store_ids = list({int(getattr(p, "store_id", 0) or 0) for p in persons if p and getattr(p, "store_id", None)})
    store_map: dict[int, KBStore] = {}
    region_map: dict[int, KBRegion] = {}

    if store_ids:
        stores = db.query(KBStore).filter(KBStore.id.in_(store_ids)).all()
        store_map = {int(s.id): s for s in stores if s and getattr(s, "id", None)}
        region_ids = list({int(getattr(s, "region_id", 0) or 0) for s in stores if s and getattr(s, "region_id", None)})
        if region_ids:
            regions = db.query(KBRegion).filter(KBRegion.id.in_(region_ids)).all()
            region_map = {int(r.id): r for r in regions if r and getattr(r, "id", None)}

    now_utc = datetime.utcnow()
    dirty = False
    items = []

    for pid in person_ids:
        p = pmap.get(int(pid))
        st = state_map.get(int(pid))

        if not p:
            items.append(
                {
                    "id": int(pid),
                    "tracked": False,
                    "latest_ts": None,
                    "seen_ts": None,
                    "is_new": False,
                    "open_url": "",
                    "error": "not_found",
                }
            )
            continue

        tracked = _get_person_diary_track(p, st)

        st_store = store_map.get(int(getattr(p, "store_id", 0) or 0))
        rg = region_map.get(int(getattr(st_store, "region_id", 0) or 0)) if st_store else None
        open_url = _build_diary_open_url_from_maps(p, st_store, rg)

        if not tracked:
            items.append(
                {
                    "id": int(pid),
                    "tracked": False,
                    "latest_ts": None,
                    "seen_ts": _get_person_diary_seen_ts(p, st),
                    "is_new": False,
                    "open_url": open_url,
                    "error": "not_tracked",
                }
            )
            continue

        # URLなしなら取れない（ただしopen_urlは返す）
        pu = ""
        if hasattr(p, "url"):
            pu = (getattr(p, "url", "") or "").strip()
        if not pu:
            items.append(
                {
                    "id": int(pid),
                    "tracked": True,
                    "latest_ts": None,
                    "seen_ts": _get_person_diary_seen_ts(p, st),
                    "is_new": False,
                    "open_url": open_url,
                    "error": "url_empty",
                }
            )
            continue

        # state行が必要になる可能性が高いので、存在するなら作っておく（checked_at等）
        st = _get_or_create_diary_state(db, state_map, int(pid)) if _diary_state_enabled() else st

        # DB間引き（checked_at が新しければ外部取得しない）
        latest_ts = _get_person_diary_latest_ts(p, st)
        checked_at = _get_person_diary_checked_at(p, st)

        need_fetch = True
        if checked_at and latest_ts is not None:
            try:
                age_sec = (now_utc - checked_at).total_seconds()
                if age_sec >= 0 and age_sec < float(_DIARY_DB_RECHECK_INTERVAL_SEC):
                    need_fetch = False
            except Exception:
                need_fetch = True

        err = ""
        if need_fetch:
            latest_ts_fetched, err = _get_latest_diary_ts_ms(pu)

            # checked_at は「試行した」時点で更新（失敗でも間引きが効く）
            if _set_person_diary_checked_at(p, now_utc, st):
                dirty = True

            if latest_ts_fetched is not None:
                if _set_person_diary_latest_ts(p, latest_ts_fetched, st):
                    dirty = True
                latest_ts = latest_ts_fetched
            else:
                # 取得できなければ latest_ts は維持（NoneならNone）
                pass

        # seen / NEW判定（仕様B: seen未設定なら初回にseen=latestで初期化）
        seen_ts = _get_person_diary_seen_ts(p, st)
        is_new = False

        if latest_ts is not None:
            if seen_ts is None:
                if _set_person_diary_seen_ts(p, latest_ts, st):
                    dirty = True
                seen_ts = latest_ts
                is_new = False
            else:
                try:
                    is_new = int(latest_ts) > int(seen_ts)
                except Exception:
                    is_new = False

        items.append(
            {
                "id": int(pid),
                "tracked": True,
                "latest_ts": latest_ts,
                "seen_ts": seen_ts,
                "is_new": bool(is_new),
                "open_url": open_url,
                "error": err,
            }
        )

    if dirty:
        try:
            db.commit()
        except Exception:
            db.rollback()

    return JSONResponse({"ok": True, "items": items})


@router.post("/kb/api/diary_seen")
async def kb_api_diary_seen(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    NEWクリック等で既読化するAPI（DB版）
    body:
      { "id": 123 }
    - seen_ts = latest_ts（無ければ失敗）
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    pid = None
    if isinstance(data, dict):
        pid = _parse_int(data.get("id", ""))
    if pid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)

    p = db.query(KBPerson).filter(KBPerson.id == int(pid)).first()
    if not p:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    state_map = _get_diary_state_map(db, [int(pid)])
    st = state_map.get(int(pid))
    if _diary_state_enabled():
        st = _get_or_create_diary_state(db, state_map, int(pid)) or st

    latest_ts = _get_person_diary_latest_ts(p, st)
    if latest_ts is None:
        return JSONResponse({"ok": False, "error": "latest_not_ready"}, status_code=409)

    changed = _set_person_diary_seen_ts(p, int(latest_ts), st)
    if not changed:
        # 列もテーブルも無いなど
        return JSONResponse({"ok": False, "error": "not_supported"}, status_code=409)

    try:
        db.commit()
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)

    return JSONResponse({"ok": True, "seen_ts": int(latest_ts)})


@router.get("/kb/api/diary_status")
def kb_api_diary_status(
    ids: str = Query(""),
    db: Session = Depends(get_db),
):
    """
    旧フロント互換用
    - DB版では is_new を返せる（tracked/seen/latest を使う）
    """
    person_ids = _parse_ids_csv(ids, limit=30)
    if not person_ids:
        return JSONResponse({"ok": True, "items": []})

    persons = (
        db.query(KBPerson)
        .filter(KBPerson.id.in_(person_ids))
        .all()
    )
    pmap = {int(getattr(p, "id", 0)): p for p in persons if p and getattr(p, "id", None)}
    state_map = _get_diary_state_map(db, person_ids)

    out = []
    for pid in person_ids:
        p = pmap.get(int(pid))
        st = state_map.get(int(pid))
        if not p:
            out.append({"id": int(pid), "is_new": False})
            continue

        if not _get_person_diary_track(p, st):
            out.append({"id": int(pid), "is_new": False})
            continue

        latest_ts = _get_person_diary_latest_ts(p, st)
        seen_ts = _get_person_diary_seen_ts(p, st)

        is_new = False
        if latest_ts is not None and seen_ts is not None:
            try:
                is_new = int(latest_ts) > int(seen_ts)
            except Exception:
                is_new = False

        out.append({"id": int(pid), "is_new": bool(is_new)})

    return JSONResponse({"ok": True, "items": out})


# =========================
# 価格テンプレ（DB版）
# =========================
def _sanitize_template_name(raw: str) -> str:
    s = unicodedata.normalize("NFKC", str(raw or "")).strip()
    s = re.sub(r"\s+", " ", s)
    if len(s) > 60:
        s = s[:60].strip()
    return s


def _sanitize_price_template_items(items) -> list[dict]:
    """
    items 期待:
    - [{"label":"基本", "amount":12000}, ...]
    """
    if not isinstance(items, list):
        return []

    out: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue

        label = unicodedata.normalize("NFKC", str(it.get("label", "") or "")).strip()
        label = re.sub(r"\s+", " ", label)
        if len(label) > 40:
            label = label[:40].strip()

        amt_raw = it.get("amount", 0)
        amt = _parse_amount_int(amt_raw)

        # 両方空は捨てる
        if not label and amt == 0:
            continue

        out.append({"label": label, "amount": int(amt)})

        if len(out) >= 40:
            break

    return out


def _utc_iso(dt) -> Optional[str]:
    if not dt:
        return None
    try:
        # DBにtimezone無しで入ってる想定（utc扱い）
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


@router.get("/kb/api/price_templates")
def kb_api_price_templates(
    store_id: str = "",
    include_global: str = "1",
    sort: str = "name",   # name | updated | created
    order: str = "",      # asc | desc（未指定は name=asc、それ以外=desc）
    db: Session = Depends(get_db),
):
    """
    store_id を指定したら:
    - store専用テンプレ + （include_global=1なら）共通テンプレ（store_id NULL）

    sort:
    - name: 名前順
    - updated: 最近使った/更新順（updated_at desc がデフォ）
    - created: 作成順（created_at desc がデフォ）

    ※「よく使う順（頻度）」を厳密にやるには use_count 等の列が必要。
      ここでは “最近使った順（updated_at）” を提供します。
    """
    sid = _parse_int(store_id)

    q = db.query(KBPriceTemplate)
    conds = []

    if sid is not None:
        conds.append(KBPriceTemplate.store_id == int(sid))
    if (include_global or "") == "1":
        conds.append(KBPriceTemplate.store_id.is_(None))

    if conds:
        q = q.filter(or_(*conds))

    sk = (sort or "").strip().lower()
    od = (order or "").strip().lower()

    if sk not in ("name", "updated", "created"):
        sk = "name"

    if od not in ("asc", "desc"):
        od = "asc" if sk == "name" else "desc"

    # store_id(NULL先頭) でグルーピングは維持しつつ、2ndで並び替え
    base_order = [KBPriceTemplate.store_id.asc().nullsfirst()]

    if sk == "name":
        base_order.append(KBPriceTemplate.name.asc() if od == "asc" else KBPriceTemplate.name.desc())
    elif sk == "created":
        col = getattr(KBPriceTemplate, "created_at", None)
        if col is not None:
            base_order.append(col.asc() if od == "asc" else col.desc())
        else:
            base_order.append(KBPriceTemplate.id.asc() if od == "asc" else KBPriceTemplate.id.desc())
    else:  # updated
        col = getattr(KBPriceTemplate, "updated_at", None)
        if col is not None:
            base_order.append(col.asc() if od == "asc" else col.desc())
        else:
            base_order.append(KBPriceTemplate.id.asc() if od == "asc" else KBPriceTemplate.id.desc())

    # 安定のため最後に name
    base_order.append(KBPriceTemplate.name.asc())

    rows = q.order_by(*base_order).all()

    items = []
    for t in rows:
        items.append(
            {
                "id": int(getattr(t, "id")),
                "store_id": getattr(t, "store_id", None),
                "name": getattr(t, "name", "") or "",
                "items": getattr(t, "items", None),
                "created_at": _utc_iso(getattr(t, "created_at", None)),
                "updated_at": _utc_iso(getattr(t, "updated_at", None)),
            }
        )

    return JSONResponse({"ok": True, "items": items})


@router.post("/kb/api/price_templates/save")
async def kb_api_price_templates_save(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    upsert（store_id + name で一意）
    body:
    {
      "store_id": 123 or null,
      "name": "基本セット",
      "items": [{"label":"基本", "amount":12000}, ...]
    }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    store_id = data.get("store_id", None)
    if store_id in ("", "null"):
        store_id = None
    sid = _parse_int(store_id) if store_id is not None else None

    name = _sanitize_template_name(data.get("name", ""))
    if not name:
        return JSONResponse({"ok": False, "error": "name_required"}, status_code=400)

    items = _sanitize_price_template_items(data.get("items", []))
    # 空でも保存は許可（「名前だけテンプレ」もアリにする）
    items_payload = items or None

    try:
        exists_q = db.query(KBPriceTemplate).filter(
            KBPriceTemplate.name == name,
            (KBPriceTemplate.store_id == int(sid)) if sid is not None else KBPriceTemplate.store_id.is_(None),
        )
        obj = exists_q.first()

        if obj:
            obj.items = items_payload
            if hasattr(obj, "updated_at"):
                obj.updated_at = datetime.utcnow()
        else:
            obj = KBPriceTemplate(
                store_id=int(sid) if sid is not None else None,
                name=name,
                items=items_payload,
                created_at=datetime.utcnow() if hasattr(KBPriceTemplate, "created_at") else None,
                updated_at=datetime.utcnow() if hasattr(KBPriceTemplate, "updated_at") else None,
            )
            db.add(obj)

        db.commit()
        db.refresh(obj)

        return JSONResponse(
            {"ok": True, "item": {"id": int(obj.id), "store_id": obj.store_id, "name": obj.name, "items": obj.items}}
        )
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)


@router.post("/kb/api/price_templates/rename")
async def kb_api_price_templates_rename(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    ② テンプレのリネームを「削除→再作成」ではなく update で安全に行う。
    body:
    {
      "id": 123,
      "name": "新しい名前"
    }

    - 同一 scope（store_idが同じ or 両方NULL）で name 重複は拒否
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    tid = _parse_int(data.get("id", ""))
    new_name = _sanitize_template_name(data.get("name", ""))

    if tid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)
    if not new_name:
        return JSONResponse({"ok": False, "error": "name_required"}, status_code=400)

    obj = db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).first()
    if not obj:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    if (getattr(obj, "name", "") or "") == new_name:
        return JSONResponse({"ok": True, "item": {"id": int(obj.id), "store_id": obj.store_id, "name": obj.name, "items": obj.items}})

    # 重複チェック（store_id scope）
    sid = getattr(obj, "store_id", None)
    dup_q = db.query(KBPriceTemplate).filter(
        KBPriceTemplate.id != int(obj.id),
        KBPriceTemplate.name == new_name,
        (KBPriceTemplate.store_id == int(sid)) if sid is not None else KBPriceTemplate.store_id.is_(None),
    )
    if dup_q.first():
        return JSONResponse({"ok": False, "error": "name_exists"}, status_code=409)

    try:
        obj.name = new_name
        if hasattr(obj, "updated_at"):
            obj.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(obj)
        return JSONResponse({"ok": True, "item": {"id": int(obj.id), "store_id": obj.store_id, "name": obj.name, "items": obj.items}})
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)


@router.post("/kb/api/price_templates/touch")
async def kb_api_price_templates_touch(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    ①の「よく使う順」の代替として “最近使った順(updated_at)” を成立させるための軽量API。
    フロントでテンプレを適用したタイミングで呼べば updated_at が更新されます。
    body: { "id": 123 }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    tid = None
    if isinstance(data, dict):
        tid = _parse_int(data.get("id", ""))
    if tid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)

    obj = db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).first()
    if not obj:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    try:
        if hasattr(obj, "updated_at"):
            obj.updated_at = datetime.utcnow()
            db.commit()
        return JSONResponse({"ok": True})
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)


@router.post("/kb/api/price_templates/delete")
async def kb_api_price_templates_delete(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    body:
    { "id": 123 }
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    tid = None
    if isinstance(data, dict):
        tid = _parse_int(data.get("id", ""))
    if tid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)

    try:
        n = db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).delete(synchronize_session=False)
        db.commit()
        return JSONResponse({"ok": True, "deleted": int(n or 0)})
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)


@router.get("/kb/api/price_templates/export")
def kb_api_price_templates_export(
    store_id: str = "",
    include_global: str = "1",
    db: Session = Depends(get_db),
):
    """
    ③ テンプレのインポート/エクスポート（テンプレ単体のバックアップ）
    - store_id 指定でスコープを絞れる
    - include_global=1 なら共通テンプレも含める
    """
    sid = _parse_int(store_id)

    q = db.query(KBPriceTemplate)
    conds = []

    if sid is not None:
        conds.append(KBPriceTemplate.store_id == int(sid))
    if (include_global or "") == "1":
        conds.append(KBPriceTemplate.store_id.is_(None))

    if conds:
        q = q.filter(or_(*conds))

    rows = q.order_by(KBPriceTemplate.store_id.asc().nullsfirst(), KBPriceTemplate.name.asc()).all()

    items = []
    for t in rows:
        items.append(
            {
                "id": int(getattr(t, "id")),
                "store_id": getattr(t, "store_id", None),
                "name": getattr(t, "name", "") or "",
                "items": getattr(t, "items", None),
            }
        )

    payload = {
        "version": 1,
        "exported_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price_templates": items,
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/kb/api/price_templates/import")
async def kb_api_price_templates_import(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    ③ テンプレのインポート（テンプレ単体）
    body:
    {
      "payload": { ... }  # /kb/api/price_templates/export のJSON
      "confirm": "1",
      "mode": "replace"   # replaceのみ（安全寄り）
    }

    - replace: 対象スコープのテンプレを全削除→流し込み
      （※payload内に store_id が混在する場合は、その混在分を丸ごと置換）
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    if (str(data.get("confirm", "")) or "") != "1":
        return JSONResponse({"ok": False, "error": "confirm_required"}, status_code=400)

    mode = (str(data.get("mode", "replace")) or "replace").strip().lower()
    if mode != "replace":
        return JSONResponse({"ok": False, "error": "mode_not_supported"}, status_code=400)

    payload = data.get("payload", None)
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    tpls = payload.get("price_templates", payload.get("templates", []))
    if not isinstance(tpls, list):
        return JSONResponse({"ok": False, "error": "templates_not_list"}, status_code=400)

    # どの store_id スコープが含まれているかを収集（None含む）
    scope_store_ids: set[Optional[int]] = set()
    for it in tpls:
        if not isinstance(it, dict):
            continue
        sid = it.get("store_id", None)
        if sid in ("", "null"):
            sid = None
        sid_i = _parse_int(sid) if sid is not None else None
        scope_store_ids.add(int(sid_i) if sid_i is not None else None)

    try:
        # スコープ内の既存テンプレを削除（FK事故回避のためテンプレだけ）
        for sid in scope_store_ids:
            if sid is None:
                db.query(KBPriceTemplate).filter(KBPriceTemplate.store_id.is_(None)).delete(synchronize_session=False)
            else:
                db.query(KBPriceTemplate).filter(KBPriceTemplate.store_id == int(sid)).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "clear_failed"}, status_code=500)

    inserted = 0
    try:
        for it in tpls:
            if not isinstance(it, dict):
                continue

            tid = _parse_int(it.get("id", ""))
            sid = it.get("store_id", None)
            if sid in ("", "null"):
                sid = None
            sid_i = _parse_int(sid) if sid is not None else None

            name = _sanitize_template_name(it.get("name", ""))
            if not name:
                continue

            items = it.get("items", None)
            items_norm = _sanitize_price_template_items(items) if items is not None else []
            items_payload = (items_norm or None) if items is not None else None

            obj = KBPriceTemplate(
                store_id=int(sid_i) if sid_i is not None else None,
                name=name,
                items=items_payload,
                created_at=datetime.utcnow() if hasattr(KBPriceTemplate, "created_at") else None,
                updated_at=datetime.utcnow() if hasattr(KBPriceTemplate, "updated_at") else None,
            )
            if tid is not None:
                try:
                    obj.id = int(tid)
                except Exception:
                    pass

            db.add(obj)
            inserted += 1

        db.flush()

        # 明示ID挿入のあと、Postgresのシーケンスを復旧
        _reset_postgres_pk_sequence(db, KBPriceTemplate)

        db.commit()
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "import_failed"}, status_code=500)

    return JSONResponse({"ok": True, "inserted": int(inserted)})


# =========================
# 並び替え・絞り込み（共通）
# =========================
SORT_OPTIONS: Dict[str, str] = {
    "name": "名前",
    "last_visit": "最近利用",
    "avg_amount": "平均金額",
    "avg_rating": "平均評価",
    "height": "身長",
    "cup": "カップ",
}


def _normalize_sort_params(sort_key: str, order: str) -> tuple[str, str]:
    sk = (sort_key or "").strip()
    od = (order or "").strip().lower()

    if sk not in SORT_OPTIONS:
        sk = "name"

    if od not in ("asc", "desc"):
        od = "asc" if sk == "name" else "desc"

    return sk, od


def _cup_rank(cup: Optional[str]) -> int:
    if not cup:
        return 0
    s = unicodedata.normalize("NFKC", str(cup)).strip().upper()
    if len(s) >= 1 and "A" <= s[0] <= "Z":
        return ord(s[0]) - ord("A") + 1
    return 0


def _parse_rating_min(x: str) -> Optional[int]:
    v = _parse_int(x)
    if v is None:
        return None
    if 1 <= v <= 5:
        return int(v)
    return None


def _last_visit_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, datetime]:
    """
    person_id -> 最終 visited_at（visited_at が NULL のログは無視）
    """
    if not person_ids:
        return {}
    rows = (
        db.query(KBVisit.person_id, func.max(KBVisit.visited_at))
        .filter(
            KBVisit.person_id.in_(person_ids),
            KBVisit.visited_at.isnot(None),
        )
        .group_by(KBVisit.person_id)
        .all()
    )
    out: dict[int, datetime] = {}
    for pid, dt in rows:
        if pid is None or dt is None:
            continue
        out[int(pid)] = dt
    return out


def _filter_persons_by_rating_min(
    persons: List[KBPerson],
    rating_min: Optional[int],
    rating_avg_map: dict[int, float],
) -> List[KBPerson]:
    if rating_min is None:
        return persons
    out: List[KBPerson] = []
    for p in persons:
        avg = rating_avg_map.get(int(p.id))
        if avg is None:
            continue
        try:
            if float(avg) >= float(rating_min):
                out.append(p)
        except Exception:
            continue
    return out


def _sort_persons(
    persons: List[KBPerson],
    sort_key: str,
    order: str,
    rating_avg_map: dict[int, float],
    amount_avg_map: dict[int, int],
    last_visit_map: dict[int, datetime],
) -> List[KBPerson]:
    sk = (sort_key or "").strip()
    od = (order or "").strip().lower()

    if sk not in SORT_OPTIONS:
        sk = "name"

    # name は基本 asc を推奨（desc も許可するが UI 的にはあまり使わない想定）
    if od not in ("asc", "desc"):
        od = "asc" if sk == "name" else "desc"

    if sk == "name":
        return sorted(
            persons,
            key=lambda p: norm_text(getattr(p, "name", "") or ""),
            reverse=(od == "desc"),
        )

    # 数値/日時系は「欠損は常に最後」になるように missing フラグを先頭に置いて、
    # order は値側に符号を掛けて “常に昇順ソート” で統一する。
    direction = -1.0 if od == "desc" else 1.0

    def metric_value(p: KBPerson) -> Optional[float]:
        pid = int(getattr(p, "id", 0) or 0)

        if sk == "avg_rating":
            v = rating_avg_map.get(pid)
            return float(v) if v is not None else None

        if sk == "avg_amount":
            v = amount_avg_map.get(pid)
            return float(v) if v is not None else None

        if sk == "height":
            v = getattr(p, "height_cm", None)
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        if sk == "cup":
            v = _cup_rank(getattr(p, "cup", None))
            return float(v) if v > 0 else None

        if sk == "last_visit":
            dt = last_visit_map.get(pid)
            if dt is None:
                return None
            try:
                return float(dt.timestamp())
            except Exception:
                return None

        return None

    def key_fn(p: KBPerson):
        mv = metric_value(p)
        missing = 1 if mv is None else 0
        vv = (mv or 0.0) * direction
        # 2nd tie-breaker: name
        nm = norm_text(getattr(p, "name", "") or "")
        return (missing, vv, nm)

    return sorted(persons, key=key_fn)


def _find_similar_persons_in_store(
    db: Session,
    store_id: int,
    name: str,
    exclude_person_id: Optional[int] = None,
    limit: int = 5,
) -> List[KBPerson]:
    """
    “ゆるく”重複っぽい人物を拾う（強制ブロックはしない）
    - 同一店舗内で比較
    - SequenceMatcher で類似度
    - contains も保険で拾う
    """
    raw = (name or "").strip()
    if not raw:
        return []
    n = norm_text(raw)
    if not n:
        return []

    rows = (
        db.query(KBPerson)
        .filter(KBPerson.store_id == int(store_id))
        .order_by(KBPerson.name.asc())
        .all()
    )

    scored: List[tuple[float, KBPerson]] = []
    for p in rows:
        pid = int(getattr(p, "id", 0) or 0)
        if exclude_person_id and pid == int(exclude_person_id):
            continue

        pn = getattr(p, "name_norm", None) or norm_text(getattr(p, "name", "") or "")
        if not pn:
            continue

        # “完全一致”は通常は exists_p で処理されるが、念のため弾く
        if pn == n:
            continue

        # contains 系
        contains_hit = (n in pn) or (pn in n)

        # 類似度
        try:
            ratio = SequenceMatcher(None, n, pn).ratio()
        except Exception:
            ratio = 0.0

        if contains_hit:
            ratio = max(ratio, 0.85)

        if ratio >= 0.78:
            scored.append((ratio, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[: max(1, int(limit))]]


# =========================
# 正規化（大文字小文字 + カタ/ひら揺らぎ対応）
# =========================
def _kata_to_hira(s: str) -> str:
    out = []
    for ch in s:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            out.append(chr(code - 0x60))
        else:
            out.append(ch)
    return "".join(out)


def norm_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    s = _kata_to_hira(s)
    return s


# =========================
# 外部検索用の正規化（※カタカナ→ひらがなはしない）
# =========================
def norm_store_kw(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    return s


# =========================
# 外部検索用：店名→タイトル検索keyword生成（〇〇店/住所っぽい塊対策）
# =========================
_STORE_STOPWORDS = {
    # 英語っぽい一般語（先頭によく付く）
    "club", "bar", "salon", "spa", "lounge", "room", "healing", "the", "and",
    "massage", "aroma", "esthetic", "esthe", "relax", "relaxation",
    # 日本語っぽい一般語
    "クラブ", "くらぶ", "バー", "さろん", "サロン", "スパ", "ラウンジ", "ルーム",
    "メンズ", "メンエス", "エステ", "マッサージ", "アロマ", "癒し",
    # 店舗枝番・住所っぽい単語（単体でもノイズ）
    "店", "本店", "支店", "別館", "新店",
    "ビル", "ビルディング", "タワー", "プラザ", "センター",
}

_STORE_STOPWORDS_NORM = {norm_store_kw(x) for x in _STORE_STOPWORDS}

_STORE_SEP_RE = re.compile(r"[ \t\r\n\u3000\(\)\[\]{}（）【】「」『』<>/\\|・\-–—_.,!?:;]+")
_STORE_ALNUM_RE = re.compile(r"^[0-9a-z]+$", re.IGNORECASE)
_STORE_ALPHA_RE = re.compile(r"^[a-z]+$", re.IGNORECASE)
_STORE_DIGITS_RE = re.compile(r"^\d+$")

_STORE_BRANCH_TAIL_RE = re.compile(r"(本店|支店|別館|新店|[0-9]+号店|店)$")
_STORE_ADDR_HINT_RE = re.compile(r"(ビル|びる|階|f|号|丁目|番地|通り|駅前|東口|西口|南口|北口)")
_STORE_PURE_BRANCH_RE = re.compile(r"^[\u3041-\u309f\u4e00-\u9fff]{1,8}(本店|支店|別館|新店|[0-9]+号店|店)$")
_STORE_BRANCH_LOC_TAIL_RE = re.compile(
    r"^(.+?)([\u3041-\u309f\u4e00-\u9fff]{1,8})(本店|支店|別館|新店|[0-9]+号店|店)$"
)


def _tokenize_store_name(raw: str) -> List[str]:
    s = norm_store_kw(raw or "")
    s = _STORE_SEP_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return []
    return [t for t in s.split(" ") if t]


def _is_stopword(tok_norm: str) -> bool:
    return (tok_norm or "") in _STORE_STOPWORDS_NORM


def _variants(tok_norm: str) -> List[str]:
    out: List[str] = []
    t = (tok_norm or "").strip()
    if not t:
        return out

    if _STORE_PURE_BRANCH_RE.match(t):
        return out

    m = _STORE_BRANCH_LOC_TAIL_RE.match(t)
    if m:
        brand = (m.group(1) or "").strip()
        if brand:
            out.append(brand)
        return out

    out.append(t)

    t2 = _STORE_BRANCH_TAIL_RE.sub("", t).strip()
    if t2 and t2 != t:
        out.append(t2)

    for suf in ["ビルディング", "ビル", "びる", "タワー", "プラザ", "センター"]:
        if t2.endswith(norm_store_kw(suf)):
            t3 = t2[: -len(norm_store_kw(suf))].strip()
            if t3:
                out.append(t3)

    seen = set()
    uniq = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _score_keyword_candidate(tok_norm: str) -> int:
    t = (tok_norm or "").strip()
    if not t:
        return -10

    if _STORE_BRANCH_TAIL_RE.search(t):
        return -999

    if _is_stopword(t):
        return -500

    if _STORE_ALPHA_RE.match(t) and 3 <= len(t) <= 14:
        return 1000 + len(t)

    if _STORE_ALNUM_RE.match(t):
        if any("a" <= ch.lower() <= "z" for ch in t) and 3 <= len(t) <= 16:
            if _STORE_DIGITS_RE.match(t):
                return -200
            return 850 + len(t)
        return -50

    base = 300

    if _STORE_ADDR_HINT_RE.search(t):
        base -= 180

    if any("0" <= ch <= "9" for ch in t):
        base -= 120

    L = len(t)
    if 2 <= L <= 6:
        base += 120 + L * 8
    elif 7 <= L <= 10:
        base += 40
    else:
        base -= 30

    return base


def _make_store_keyword(store_name: str) -> str:
    raw = (store_name or "").strip()
    if not raw:
        return ""

    toks = _tokenize_store_name(raw)
    if not toks:
        return ""

    candidates: List[str] = []
    for t in toks:
        if _is_stopword(t):
            continue
        for v in _variants(t):
            v = (v or "").strip()
            if not v:
                continue
            if _STORE_BRANCH_TAIL_RE.search(v):
                continue
            if _is_stopword(v):
                continue
            candidates.append(v)

    if not candidates:
        s = norm_store_kw(raw)
        s = _STORE_BRANCH_TAIL_RE.sub("", s).strip()
        s = s[:8] if len(s) >= 8 else s
        return s

    best = None
    best_score = -10**9
    for c in candidates:
        sc = _score_keyword_candidate(c)
        if sc > best_score:
            best_score = sc
            best = c

    if not best or len(best) <= 1:
        s = norm_store_kw(raw)
        s = _STORE_BRANCH_TAIL_RE.sub("", s).strip()
        return s[:6] if len(s) > 6 else s

    return best


def _sanitize_image_urls(raw: str) -> list[str]:
    if not raw:
        return []

    lines = raw.splitlines()
    out = []
    seen = set()
    for line in lines:
        u = (line or "").strip()
        if not u:
            continue
        if not (u.startswith("http://") or u.startswith("https://")):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= 20:
            break
    return out


def _split_tokens(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    s = unicodedata.normalize("NFKC", raw)
    parts = re.split(r"[,\n\r\t/、，]+", s)
    out = []
    for p in parts:
        t = (p or "").strip()
        if not t:
            continue
        out.append(t)
    return out


def _token_set_norm(raw: Optional[str]) -> set:
    toks = _split_tokens(raw)
    return {norm_text(t) for t in toks if t}


def _collect_service_tag_options(db: Session) -> Tuple[List[str], List[str]]:
    persons = db.query(KBPerson.services, KBPerson.tags).all()

    svc_map: Dict[str, str] = {}
    tag_map: Dict[str, str] = {}

    for services, tags in persons:
        for t in _split_tokens(services):
            k = norm_text(t)
            if k and k not in svc_map:
                svc_map[k] = t
        for t in _split_tokens(tags):
            k = norm_text(t)
            if k and k not in tag_map:
                tag_map[k] = t

    svc_list = sorted(svc_map.values(), key=lambda x: norm_text(x))
    tag_list = sorted(tag_map.values(), key=lambda x: norm_text(x))
    return svc_list, tag_list


def _cup_letter(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = unicodedata.normalize("NFKC", raw).upper()
    m = re.search(r"[A-Z]", s)
    return m.group(0) if m else ""


def _cup_bucket_hit(bucket: str, cup_letter: str) -> bool:
    if not cup_letter:
        return False
    c = cup_letter
    if bucket == "leD":
        return "A" <= c <= "D"
    if bucket == "EF":
        return c in {"E", "F"}
    if bucket == "geG":
        return "G" <= c <= "Z"
    return False


def build_person_search_blob(db: Session, p: KBPerson) -> str:
    store = db.query(KBStore).filter(KBStore.id == p.store_id).first()
    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first() if store else None

    parts = []
    if region and region.name:
        parts.append(region.name)
    if store and store.name:
        parts.append(store.name)

    img_parts = []
    try:
        if isinstance(getattr(p, "image_urls", None), list):
            img_parts = [str(x or "") for x in (p.image_urls or [])]
    except Exception:
        img_parts = []

    parts.extend(
        [
            p.name or "",
            str(p.age or ""),
            str(p.height_cm or ""),
            p.cup or "",
            str(p.bust_cm or ""),
            str(p.waist_cm or ""),
            str(p.hip_cm or ""),
            p.services or "",
            p.tags or "",
            getattr(p, "url", "") or "",
            " ".join([x for x in img_parts if x]),
            p.memo or "",
        ]
    )
    return norm_text(" ".join([x for x in parts if x is not None]))


def build_visit_search_blob(v: KBVisit) -> str:
    parts = [v.memo or ""]
    if isinstance(v.price_items, list):
        for it in v.price_items:
            if isinstance(it, dict):
                parts.append(str(it.get("label", "") or ""))
                parts.append(str(it.get("amount", "") or ""))
    return norm_text(" ".join(parts))


def _parse_int(x: str):
    s = unicodedata.normalize("NFKC", str(x or "")).strip()
    if not s:
        return None

    s = s.replace(",", "")
    s = s.replace("_", "")
    s = re.sub(r"[ \t\u3000]", "", s)
    s = s.replace("円", "")
    s = s.replace("￥", "").replace("¥", "")

    m = re.search(r"-?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _parse_amount_int(x) -> int:
    v = _parse_int(str(x))
    if v is None:
        return 0
    if v < 0:
        return 0
    return int(v)


def _parse_time_hhmm_to_min(x: str):
    x = (x or "").strip()
    if not x:
        return None
    try:
        hh, mm = x.split(":")
        h = int(hh)
        m = int(mm)
        if h < 0 or h > 23 or m < 0 or m > 59:
            return None
        return h * 60 + m
    except Exception:
        return None


def _parse_minutes_or_hhmm(x) -> Optional[int]:
    """
    インポート互換用：
    - "540" などの分（int/str）
    - "09:00" などの時刻（HH:MM）→ 分に変換
    - "2:30" のような duration っぽい表現も分にする（2*60+30）
    """
    if x is None:
        return None
    s = unicodedata.normalize("NFKC", str(x)).strip()
    if not s:
        return None
    if ":" in s:
        # HH:MM（時刻/時間長）として扱う
        v = _parse_time_hhmm_to_min(s)
        if v is None:
            return None
        return int(v)
    v = _parse_int(s)
    if v is None:
        return None
    if v < 0:
        return None
    return int(v)


def _calc_duration(start_min, end_min):
    if start_min is None or end_min is None:
        return None
    d = end_min - start_min
    if d < 0:
        d += 24 * 60
    return d


def _avg_rating_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, float]:
    if not person_ids:
        return {}
    rows = (
        db.query(KBVisit.person_id, func.avg(KBVisit.rating))
        .filter(
            KBVisit.person_id.in_(person_ids),
            KBVisit.rating.isnot(None),
        )
        .group_by(KBVisit.person_id)
        .all()
    )
    out = {}
    for pid, avg in rows:
        if pid is None or avg is None:
            continue
        out[int(pid)] = float(avg)
    return out


def _avg_amount_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, int]:
    if not person_ids:
        return {}
    rows = (
        db.query(KBVisit.person_id, func.avg(KBVisit.total_yen))
        .filter(
            KBVisit.person_id.in_(person_ids),
            KBVisit.total_yen.isnot(None),
            KBVisit.total_yen > 0,
        )
        .group_by(KBVisit.person_id)
        .all()
    )
    out: dict[int, int] = {}
    for pid, avg in rows:
        if pid is None or avg is None:
            continue
        try:
            out[int(pid)] = int(round(float(avg)))
        except Exception:
            continue
    return out


def _build_tree_data(db: Session):
    regions = db.query(KBRegion).order_by(KBRegion.name.asc()).all()

    store_rows = (
        db.query(KBStore, KBRegion)
        .join(KBRegion, KBRegion.id == KBStore.region_id)
        .order_by(KBRegion.name.asc(), KBStore.name.asc())
        .all()
    )
    stores_by_region = defaultdict(list)
    for s, r in store_rows:
        stores_by_region[r.id].append(s)

    counts = dict(
        db.query(KBPerson.store_id, func.count(KBPerson.id)).group_by(KBPerson.store_id).all()
    )
    return regions, stores_by_region, counts


def _build_store_region_maps(db: Session, persons: List[KBPerson]):
    store_ids = list({p.store_id for p in persons})
    stores = {}
    regions_map = {}
    if store_ids:
        st_rows = db.query(KBStore).filter(KBStore.id.in_(store_ids)).all()
        for s in st_rows:
            stores[s.id] = s
        reg_ids = list({s.region_id for s in st_rows})
        if reg_ids:
            rr = db.query(KBRegion).filter(KBRegion.id.in_(reg_ids)).all()
            for r in rr:
                regions_map[r.id] = r
    return stores, regions_map


def _reset_postgres_pk_sequence(db: Session, model) -> None:
    """
    Postgres系（Neonなど）で、明示ID挿入後にシーケンスがズレる問題を直す。
    SQLiteなどでは失敗しても握りつぶす（影響なし）。
    """
    try:
        table = model.__table__.name
        pk_cols = list(model.__table__.primary_key.columns)
        if not pk_cols:
            return
        pk = pk_cols[0].name
        sql = (
            f"SELECT setval("
            f"pg_get_serial_sequence('{table}', '{pk}'), "
            f"COALESCE((SELECT MAX({pk}) FROM {table}), 1)"
            f")"
        )
        db.execute(text(sql))
    except Exception:
        return


# =========================
# KBトップ
# =========================
@router.get("/kb", response_class=HTMLResponse)
def kb_index(request: Request, db: Session = Depends(get_db)):
    regions, stores_by_region, counts = _build_tree_data(db)
    panic = request.query_params.get("panic") or ""
    search_error = request.query_params.get("search_error") or ""
    import_status = request.query_params.get("import") or ""
    import_error = request.query_params.get("import_error") or ""

    svc_options, tag_options = _collect_service_tag_options(db)

    # UI上の初期値（テンプレ側でも使う）
    sort_eff, order_eff = _normalize_sort_params("name", "asc")

    return templates.TemplateResponse(
        "kb_index.html",
        {
            "request": request,
            "regions": regions,
            "stores_by_region": stores_by_region,
            "person_counts": counts,
            "panic": panic,
            "search_error": search_error,
            "import_status": import_status,
            "import_error": import_error,
            "search_q": "",
            "search_region_id": "",
            "search_budget_min": "",
            "search_budget_max": "",
            "search_age": [],
            "search_height": [],
            "search_cup": [],
            "search_waist": [],
            "search_svc": [],
            "search_tag": [],
            "search_results": None,
            "search_truncated": False,
            "search_total_count": 0,
            "stores_map": {},
            "regions_map": {},
            "rating_avg_map": {},
            "amount_avg_map": {},
            "last_visit_map": {},
            "svc_options": svc_options,
            "tag_options": tag_options,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
            # 並び替え/絞り込みUI用（kb_index.html 側で使う）
            "sort_options": SORT_OPTIONS,
            "sort": sort_eff,
            "order": order_eff,
            "rating_min": "",
            "star_only": "",
        },
    )


@router.post("/kb/region")
def kb_add_region(request: Request, name: str = Form(""), db: Session = Depends(get_db)):
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url="/kb", status_code=303)

    try:
        exists_r = db.query(KBRegion).filter(KBRegion.name == name).first()
        if not exists_r:
            r = KBRegion(name=name, name_norm=norm_text(name))
            db.add(r)
            db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url="/kb", status_code=303)


@router.post("/kb/store")
def kb_add_store(
    request: Request,
    region_id: int = Form(...),
    name: str = Form(""),
    db: Session = Depends(get_db),
):
    name = (name or "").strip()
    if not name:
        return RedirectResponse(url="/kb", status_code=303)

    try:
        exists_s = (
            db.query(KBStore)
            .filter(KBStore.region_id == int(region_id), KBStore.name == name)
            .first()
        )
        if not exists_s:
            s = KBStore(region_id=int(region_id), name=name, name_norm=norm_text(name))
            db.add(s)
            db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url="/kb", status_code=303)


# =========================
# 店舗ページ
# =========================
@router.get("/kb/store/{store_id}", response_class=HTMLResponse)
def kb_store_page(
    request: Request,
    store_id: int,
    sort: str = "name",
    order: str = "",
    rating_min: str = "",
    star_only: str = "",
    db: Session = Depends(get_db),
):
    store = db.query(KBStore).filter(KBStore.id == int(store_id)).first()
    if not store:
        return RedirectResponse(url="/kb", status_code=303)

    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first()

    persons = (
        db.query(KBPerson)
        .filter(KBPerson.store_id == store.id)
        .order_by(KBPerson.name.asc())
        .all()
    )

    person_ids = [int(p.id) for p in persons]
    rating_avg_map = _avg_rating_map_for_person_ids(db, person_ids)
    amount_avg_map = _avg_amount_map_for_person_ids(db, person_ids)
    last_visit_map = _last_visit_map_for_person_ids(db, person_ids)

    sort_eff, order_eff = _normalize_sort_params(sort, order)

    rmin = _parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = _filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = _sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

    return templates.TemplateResponse(
        "kb_store.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "persons": persons,
            "rating_avg_map": rating_avg_map,
            "amount_avg_map": amount_avg_map,
            "last_visit_map": last_visit_map,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
            # UI用
            "sort_options": SORT_OPTIONS,
            "sort": sort_eff,
            "order": order_eff,
            "rating_min": str(rmin) if rmin is not None else "",
            "star_only": "1" if (star_only or "") == "1" else "",
        },
    )


@router.post("/kb/person")
def kb_add_person(
    request: Request,
    store_id: int = Form(...),
    name: str = Form(""),
    db: Session = Depends(get_db),
):
    name = (name or "").strip()
    back_url = request.headers.get("referer") or "/kb"
    if not name:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        exists_p = (
            db.query(KBPerson)
            .filter(KBPerson.store_id == int(store_id), KBPerson.name == name)
            .first()
        )
        if exists_p:
            # “ゆるく警告”用に類似候補を計算（自身は除外）
            dup = _find_similar_persons_in_store(
                db, int(store_id), name, exclude_person_id=int(exists_p.id)
            )
            dup_ids = ",".join([str(int(x.id)) for x in dup if x and getattr(x, "id", None)])
            url = f"/kb/person/{exists_p.id}"
            if dup_ids:
                url += "?dup=" + dup_ids
            return RedirectResponse(url=url, status_code=303)

        # 作成前に候補を拾う（新規IDはまだないので除外なし）
        dup = _find_similar_persons_in_store(db, int(store_id), name, exclude_person_id=None)

        p = KBPerson(store_id=int(store_id), name=name)
        p.name_norm = norm_text(name)
        p.search_norm = build_person_search_blob(db, p)

        db.add(p)
        db.commit()
        db.refresh(p)

        dup_ids = ",".join([str(int(x.id)) for x in dup if x and getattr(x, "id", None)])
        url = f"/kb/person/{p.id}"
        if dup_ids:
            url += "?dup=" + dup_ids
        return RedirectResponse(url=url, status_code=303)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


# =========================
# 人物ページ
# =========================
@router.get("/kb/person/{person_id}", response_class=HTMLResponse)
def kb_person_page(
    request: Request,
    person_id: int,
    dup: str = "",
    db: Session = Depends(get_db),
):
    person = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not person:
        return RedirectResponse(url="/kb", status_code=303)

    store = db.query(KBStore).filter(KBStore.id == person.store_id).first()
    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first() if store else None

    visits = (
        db.query(KBVisit)
        .filter(KBVisit.person_id == person.id)
        .order_by(desc(KBVisit.visited_at).nullslast(), desc(KBVisit.id))
        .all()
    )

    rating_avg = (
        db.query(func.avg(KBVisit.rating))
        .filter(KBVisit.person_id == person.id, KBVisit.rating.isnot(None))
        .scalar()
    )

    amount_avg = (
        db.query(func.avg(KBVisit.total_yen))
        .filter(
            KBVisit.person_id == person.id,
            KBVisit.total_yen.isnot(None),
            KBVisit.total_yen > 0,
        )
        .scalar()
    )

    amount_avg_yen = None
    try:
        if amount_avg is not None:
            amount_avg_yen = int(round(float(amount_avg)))
    except Exception:
        amount_avg_yen = None

    # ---- 重複警告表示（dup クエリで指定された person_id 群）
    dup_candidates: List[KBPerson] = []
    if dup:
        ids: List[int] = []
        for x in str(dup).split(","):
            x = (x or "").strip()
            if not x:
                continue
            v = _parse_int(x)
            if v is None:
                continue
            if int(v) == int(person.id):
                continue
            ids.append(int(v))
            if len(ids) >= 10:
                break
        if ids:
            try:
                dup_candidates = (
                    db.query(KBPerson)
                    .filter(KBPerson.id.in_(ids))
                    .order_by(KBPerson.name.asc())
                    .all()
                )
            except Exception:
                dup_candidates = []

    # ---- 外部検索ワンクリック集（Google / site検索）
    base_parts = []
    if region and region.name:
        base_parts.append(region.name)
    if store and store.name:
        base_parts.append(store.name)
    if person and person.name:
        base_parts.append(person.name)
    base_q = " ".join([x for x in base_parts if x]).strip()

    google_all_url = _build_google_search_url(base_q)
    google_cityheaven_url = _build_google_site_search_url("cityheaven.net", base_q)
    google_dto_url = _build_google_site_search_url("dto.jp", base_q)

    # 「写メ日記」も検索に含めたバージョン
    diary_q = (base_q + " 写メ日記").strip() if base_q else ""
    google_all_diary_url = _build_google_search_url(diary_q)
    google_cityheaven_diary_url = _build_google_site_search_url("cityheaven.net", diary_q)
    google_dto_diary_url = _build_google_site_search_url("dto.jp", diary_q)

    return templates.TemplateResponse(
        "kb_person.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "person": person,
            "visits": visits,
            "rating_avg": rating_avg,
            "amount_avg_yen": amount_avg_yen,
            "dup_candidates": dup_candidates,
            "google_all_url": google_all_url,
            "google_cityheaven_url": google_cityheaven_url,
            "google_dto_url": google_dto_url,
            "google_all_diary_url": google_all_diary_url,
            "google_cityheaven_diary_url": google_cityheaven_diary_url,
            "google_dto_diary_url": google_dto_diary_url,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
        },
    )


# =========================
# 人物→外部検索へ（専用リダイレクト）
# =========================
@router.get("/kb/person/{person_id}/external_search")
def kb_person_external_search(person_id: int, db: Session = Depends(get_db)):
    person = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    store = db.query(KBStore).filter(KBStore.id == person.store_id).first()

    store_name = (store.name or "").strip() if store else ""
    person_name = (person.name or "").strip()

    store_kw = _make_store_keyword(store_name)
    params = {"keyword": store_kw}

    params["no_log"] = "1"
    params["kb"] = "1"

    if person_name:
        params["post_kw"] = person_name

    if store:
        area = getattr(store, "area", None)
        board_category = getattr(store, "board_category", None)
        board_id = getattr(store, "board_id", None)
        if area:
            params["area"] = area
        if board_category:
            params["board_category"] = board_category
        if board_id:
            params["board_id"] = board_id

    url = "/thread_search?" + urlencode(params)
    return RedirectResponse(url=url, status_code=303)


@router.post("/kb/person/{person_id}/update")
def kb_update_person(
    request: Request,
    person_id: int,
    name: str = Form(""),
    age: str = Form(""),
    height_cm: str = Form(""),
    cup: str = Form(""),
    bust_cm: str = Form(""),
    waist_cm: str = Form(""),
    hip_cm: str = Form(""),
    services: str = Form(""),
    tags: str = Form(""),
    url: str = Form(""),
    image_urls_text: str = Form(""),
    memo: str = Form(""),
    # ★追加：写メ日記追跡
    diary_track: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    # state（存在するなら）
    state_map = _get_diary_state_map(db, [int(person_id)])
    st = state_map.get(int(person_id))
    if _diary_state_enabled():
        st = _get_or_create_diary_state(db, state_map, int(person_id)) or st

    try:
        p.name = (name or "").strip() or p.name
        p.age = _parse_int(age)
        p.height_cm = _parse_int(height_cm)

        cu = (cup or "").strip()
        cu = unicodedata.normalize("NFKC", cu).upper()
        p.cup = (cu[:1] if cu and "A" <= cu[:1] <= "Z" else None)

        p.bust_cm = _parse_int(bust_cm)
        p.waist_cm = _parse_int(waist_cm)
        p.hip_cm = _parse_int(hip_cm)

        p.services = (services or "").strip() or None
        p.tags = (tags or "").strip() or None

        if hasattr(p, "url"):
            u = (url or "").strip()
            p.url = u or None
            if hasattr(p, "url_norm"):
                p.url_norm = norm_text(p.url or "")

        if hasattr(p, "image_urls"):
            urls = _sanitize_image_urls(image_urls_text or "")
            p.image_urls = urls or None

        p.memo = (memo or "").strip() or None

        # 追跡フラグ（state優先、無ければKBPerson列）
        new_track = _bool_from_form(diary_track)
        old_track = _get_person_diary_track(p, st)

        if _set_person_diary_track(p, new_track, st):
            # OFF→ONの初回は「仕様B」に寄せる：
            # seen_ts が未設定なら、そのまま（最初のチェック時に seen=latest で初期化し NEWを出さない）
            if (not old_track) and new_track:
                # 次のチェックで拾えるように checked_at を落としておく
                _set_person_diary_checked_at(p, None, st)

        p.name_norm = norm_text(p.name or "")
        p.services_norm = norm_text(p.services or "")
        p.tags_norm = norm_text(p.tags or "")
        p.memo_norm = norm_text(p.memo or "")

        p.search_norm = build_person_search_blob(db, p)

        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/person/{person_id}/visit")
def kb_add_visit(
    request: Request,
    person_id: int,
    visited_at: str = Form(""),
    start_time: str = Form(""),
    end_time: str = Form(""),
    rating: str = Form(""),
    memo: str = Form(""),
    price_items_json: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    dt = None
    vd = (visited_at or "").strip()
    if vd:
        try:
            dt = datetime.strptime(vd, "%Y-%m-%d")
        except Exception:
            dt = None

    smin = _parse_time_hhmm_to_min(start_time)
    emin = _parse_time_hhmm_to_min(end_time)
    dur = _calc_duration(smin, emin)

    r = None
    try:
        rr = int((rating or "").strip() or "0")
        if 1 <= rr <= 5:
            r = rr
    except Exception:
        r = None

    items = []
    total = 0
    raw = (price_items_json or "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for it in data:
                    if not isinstance(it, dict):
                        continue
                    label = str(it.get("label", "") or "").strip()
                    amt = it.get("amount", 0)
                    amt_i = _parse_amount_int(amt)

                    if not label and amt_i == 0:
                        continue
                    items.append({"label": label, "amount": amt_i})
                    total += amt_i
        except Exception:
            items = []
            total = 0

    try:
        v = KBVisit(
            person_id=p.id,
            visited_at=dt,
            start_time=smin,
            end_time=emin,
            duration_min=dur,
            rating=r,
            memo=(memo or "").strip() or None,
            price_items=items or None,
            total_yen=int(total),
        )
        v.search_norm = build_visit_search_blob(v)
        db.add(v)
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/visit/{visit_id}/update")
def kb_update_visit(
    request: Request,
    visit_id: int,
    visited_at: str = Form(""),
    start_time: str = Form(""),
    end_time: str = Form(""),
    rating: str = Form(""),
    memo: str = Form(""),
    price_items_json: str = Form(""),
    db: Session = Depends(get_db),
):
    v = db.query(KBVisit).filter(KBVisit.id == int(visit_id)).first()
    if not v:
        return RedirectResponse(url="/kb", status_code=303)

    back_url = request.headers.get("referer") or f"/kb/person/{v.person_id}"

    dt = None
    vd = (visited_at or "").strip()
    if vd:
        try:
            dt = datetime.strptime(vd, "%Y-%m-%d")
        except Exception:
            dt = None

    smin = _parse_time_hhmm_to_min(start_time)
    emin = _parse_time_hhmm_to_min(end_time)
    dur = _calc_duration(smin, emin)

    r = None
    try:
        rr = int((rating or "").strip() or "0")
        if 1 <= rr <= 5:
            r = rr
    except Exception:
        r = None

    raw = (price_items_json or "").strip()

    update_price = False
    new_items = None
    new_total = None

    if raw == "":
        update_price = False
    elif raw == "[]":
        update_price = True
        new_items = None
        new_total = 0
    else:
        try:
            data = json.loads(raw)
            items = []
            total = 0
            if isinstance(data, list):
                for it in data:
                    if not isinstance(it, dict):
                        continue
                    label = str(it.get("label", "") or "").strip()
                    amt = it.get("amount", 0)
                    amt_i = _parse_amount_int(amt)

                    if not label and amt_i == 0:
                        continue
                    items.append({"label": label, "amount": amt_i})
                    total += amt_i
            update_price = True
            new_items = items or None
            new_total = int(total)
        except Exception:
            update_price = False

    try:
        v.visited_at = dt
        v.start_time = smin
        v.end_time = emin
        v.duration_min = dur
        v.rating = r
        v.memo = (memo or "").strip() or None

        if update_price:
            v.price_items = new_items
            v.total_yen = int(new_total or 0)

        v.search_norm = build_visit_search_blob(v)

        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/visit/{visit_id}/delete")
def kb_delete_visit(request: Request, visit_id: int, db: Session = Depends(get_db)):
    back_url = request.headers.get("referer") or "/kb"
    try:
        db.query(KBVisit).filter(KBVisit.id == int(visit_id)).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/person/{person_id}/delete")
def kb_delete_person(request: Request, person_id: int, db: Session = Depends(get_db)):
    back_url = request.headers.get("referer") or "/kb"
    try:
        db.query(KBVisit).filter(KBVisit.person_id == int(person_id)).delete(synchronize_session=False)
        db.query(KBPerson).filter(KBPerson.id == int(person_id)).delete(synchronize_session=False)
        if _diary_state_enabled():
            try:
                db.query(KBDiaryState).filter(KBDiaryState.person_id == int(person_id)).delete(synchronize_session=False)  # type: ignore
            except Exception:
                pass
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/panic_delete_all")
def kb_panic_delete_all(
    request: Request,
    confirm_check: str = Form(""),
    db: Session = Depends(get_db),
):
    if (confirm_check or "") != "1":
        return RedirectResponse(url="/kb?panic=failed", status_code=303)

    try:
        db.query(KBPriceTemplate).delete(synchronize_session=False)
        db.query(KBVisit).delete(synchronize_session=False)
        db.query(KBPerson).delete(synchronize_session=False)
        db.query(KBStore).delete(synchronize_session=False)
        db.query(KBRegion).delete(synchronize_session=False)
        if _diary_state_enabled():
            try:
                db.query(KBDiaryState).delete(synchronize_session=False)  # type: ignore
            except Exception:
                pass
        db.commit()
    except Exception:
        db.rollback()
        return RedirectResponse(url="/kb?panic=failed", status_code=303)

    return RedirectResponse(url="/kb?panic=done", status_code=303)


# =========================
# 人物検索（詳細検索 + フリーワード）
# =========================
@router.get("/kb/search", response_class=HTMLResponse)
def kb_search(
    request: Request,
    db: Session = Depends(get_db),
    q: str = "",
    region_id: str = "",
    budget_min: str = "",
    budget_max: str = "",
    age: List[str] = Query(default=[]),
    height: List[str] = Query(default=[]),
    cup: List[str] = Query(default=[]),
    waist: List[str] = Query(default=[]),
    svc: List[str] = Query(default=[]),
    tag: List[str] = Query(default=[]),
    # ★追加：並び替え/★絞り込み（kb_index.html でUIを付ける）
    sort: str = "name",
    order: str = "",
    rating_min: str = "",
    star_only: str = "",
):
    regions, stores_by_region, counts = _build_tree_data(db)
    svc_options, tag_options = _collect_service_tag_options(db)

    rid = _parse_int(region_id)
    bmin = _parse_int(budget_min)
    bmax = _parse_int(budget_max)

    q_raw = (q or "").strip()
    qn = norm_text(q_raw) if q_raw else ""

    person_q = db.query(KBPerson)

    if rid:
        person_q = (
            person_q.join(KBStore, KBStore.id == KBPerson.store_id)
            .join(KBRegion, KBRegion.id == KBStore.region_id)
            .filter(KBRegion.id == rid)
        )

    age_conds = []
    if age:
        for a in age:
            if a == "u20":
                age_conds.append(KBPerson.age.isnot(None) & (KBPerson.age <= 20))
            elif a == "21_23":
                age_conds.append(KBPerson.age.isnot(None) & KBPerson.age.between(21, 23))
            elif a == "24_25":
                age_conds.append(KBPerson.age.isnot(None) & KBPerson.age.between(24, 25))
            elif a == "ge26":
                age_conds.append(KBPerson.age.isnot(None) & (KBPerson.age >= 26))
    if age_conds:
        person_q = person_q.filter(or_(*age_conds))

    height_conds = []
    if height:
        for h in height:
            if h == "le149":
                height_conds.append(KBPerson.height_cm.isnot(None) & (KBPerson.height_cm <= 149))
            elif h == "150_158":
                height_conds.append(
                    KBPerson.height_cm.isnot(None) & KBPerson.height_cm.between(150, 158)
                )
            elif h == "ge159":
                height_conds.append(KBPerson.height_cm.isnot(None) & (KBPerson.height_cm >= 159))
    if height_conds:
        person_q = person_q.filter(or_(*height_conds))

    waist_conds = []
    if waist:
        for w in waist:
            if w == "le49":
                waist_conds.append(KBPerson.waist_cm.isnot(None) & (KBPerson.waist_cm <= 49))
            elif w == "50_56":
                waist_conds.append(KBPerson.waist_cm.isnot(None) & KBPerson.waist_cm.between(50, 56))
            elif w == "57_59":
                waist_conds.append(KBPerson.waist_cm.isnot(None) & KBPerson.waist_cm.between(57, 59))
            elif w == "ge60":
                waist_conds.append(KBPerson.waist_cm.isnot(None) & (KBPerson.waist_cm >= 60))
    if waist_conds:
        person_q = person_q.filter(or_(*waist_conds))

    if bmin is not None or bmax is not None:
        conds = [KBVisit.person_id == KBPerson.id]
        if bmin is not None:
            conds.append(KBVisit.total_yen >= int(bmin))
        if bmax is not None:
            conds.append(KBVisit.total_yen <= int(bmax))
        budget_exists = exists().where(and_(*conds))
        person_q = person_q.filter(budget_exists)

    if qn:
        visit_exists = exists().where(
            and_(
                KBVisit.person_id == KBPerson.id,
                KBVisit.search_norm.isnot(None),
                KBVisit.search_norm.contains(qn),
            )
        )
        person_q = person_q.filter(
            or_(
                and_(KBPerson.search_norm.isnot(None), KBPerson.search_norm.contains(qn)),
                visit_exists,
            )
        )

    candidates = person_q.order_by(KBPerson.name.asc()).limit(2000).all()

    svc_norm_set = {norm_text(x) for x in (svc or []) if (x or "").strip()}
    tag_norm_set = {norm_text(x) for x in (tag or []) if (x or "").strip()}

    def hit_svc(p: KBPerson) -> bool:
        if not svc_norm_set:
            return True
        ps = _token_set_norm(getattr(p, "services", None))
        return any(x in ps for x in svc_norm_set)

    def hit_tag(p: KBPerson) -> bool:
        if not tag_norm_set:
            return True
        pt = _token_set_norm(getattr(p, "tags", None))
        return any(x in pt for x in tag_norm_set)

    def hit_cup(p: KBPerson) -> bool:
        if not cup:
            return True
        c = _cup_letter(getattr(p, "cup", None))
        return any(_cup_bucket_hit(b, c) for b in cup)

    persons = [p for p in candidates if hit_svc(p) and hit_tag(p) and hit_cup(p)]

    truncated = False
    total_count = len(persons)
    if len(persons) > 500:
        persons = persons[:500]
        truncated = True

    stores_map, regions_map = _build_store_region_maps(db, persons)
    rating_avg_map = _avg_rating_map_for_person_ids(db, [p.id for p in persons])
    amount_avg_map = _avg_amount_map_for_person_ids(db, [p.id for p in persons])
    last_visit_map = _last_visit_map_for_person_ids(db, [p.id for p in persons])

    sort_eff, order_eff = _normalize_sort_params(sort, order)

    rmin = _parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = _filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = _sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

    return templates.TemplateResponse(
        "kb_index.html",
        {
            "request": request,
            "regions": regions,
            "stores_by_region": stores_by_region,
            "person_counts": counts,
            "panic": request.query_params.get("panic") or "",
            "search_error": "",
            "import_status": request.query_params.get("import") or "",
            "import_error": request.query_params.get("import_error") or "",
            "search_q": q_raw,
            "search_region_id": rid or "",
            "search_budget_min": str(bmin) if bmin is not None else "",
            "search_budget_max": str(bmax) if bmax is not None else "",
            "search_age": age or [],
            "search_height": height or [],
            "search_cup": cup or [],
            "search_waist": waist or [],
            "search_svc": svc or [],
            "search_tag": tag or [],
            "search_results": persons,
            "search_truncated": truncated,
            "search_total_count": total_count,
            "stores_map": stores_map,
            "regions_map": regions_map,
            "rating_avg_map": rating_avg_map,
            "amount_avg_map": amount_avg_map,
            "last_visit_map": last_visit_map,
            "svc_options": svc_options,
            "tag_options": tag_options,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
            # UI用
            "sort_options": SORT_OPTIONS,
            "sort": sort_eff,
            "order": order_eff,
            "rating_min": str(rmin) if rmin is not None else "",
            "star_only": "1" if (star_only or "") == "1" else "",
        },
    )


# =========================
# ⑤ バックアップ（エンドポイント）
# =========================
@router.get("/kb/export")
def kb_export(db: Session = Depends(get_db)):
    """
    全KBを JSON で吐き出す（コピペ/移行/保険）
    - #026: diaryは「stateテーブルがあればそれを優先」して含める
    """
    regions = db.query(KBRegion).order_by(KBRegion.id.asc()).all()
    stores = db.query(KBStore).order_by(KBStore.id.asc()).all()
    persons = db.query(KBPerson).order_by(KBPerson.id.asc()).all()
    visits = db.query(KBVisit).order_by(KBVisit.id.asc()).all()
    price_templates = db.query(KBPriceTemplate).order_by(KBPriceTemplate.id.asc()).all()

    person_ids = [int(getattr(p, "id")) for p in persons if p and getattr(p, "id", None)]
    state_map = _get_diary_state_map(db, person_ids)

    def region_to_dict(r: KBRegion) -> dict:
        return {
            "id": int(getattr(r, "id")),
            "name": getattr(r, "name", None),
        }

    def store_to_dict(s: KBStore) -> dict:
        d = {
            "id": int(getattr(s, "id")),
            "region_id": int(getattr(s, "region_id")),
            "name": getattr(s, "name", None),
        }
        # あるなら一緒に吐く（なくてもOK）
        for k in ["area", "board_category", "board_id"]:
            if hasattr(s, k):
                d[k] = getattr(s, k)
        return d

    def person_to_dict(p: KBPerson) -> dict:
        pid = int(getattr(p, "id"))
        st = state_map.get(pid)

        d = {
            "id": pid,
            "store_id": int(getattr(p, "store_id")),
            "name": getattr(p, "name", None),
            "age": getattr(p, "age", None),
            "height_cm": getattr(p, "height_cm", None),
            "cup": getattr(p, "cup", None),
            "bust_cm": getattr(p, "bust_cm", None),
            "waist_cm": getattr(p, "waist_cm", None),
            "hip_cm": getattr(p, "hip_cm", None),
            "services": getattr(p, "services", None),
            "tags": getattr(p, "tags", None),
            "memo": getattr(p, "memo", None),
        }
        if hasattr(p, "url"):
            d["url"] = getattr(p, "url", None)
        if hasattr(p, "image_urls"):
            d["image_urls"] = getattr(p, "image_urls", None)

        # diary fields（state優先、無ければperson列）
        d["diary_track"] = bool(_get_person_diary_track(p, st))
        d["diary_latest_ts_ms"] = _get_person_diary_latest_ts(p, st)
        d["diary_seen_ts_ms"] = _get_person_diary_seen_ts(p, st)

        dt = _get_person_diary_checked_at(p, st)
        try:
            d["diary_checked_at_utc"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None
        except Exception:
            d["diary_checked_at_utc"] = None

        return d

    def visit_to_dict(v: KBVisit) -> dict:
        dt = getattr(v, "visited_at", None)
        return {
            "id": int(getattr(v, "id")),
            "person_id": int(getattr(v, "person_id")),
            "visited_at": dt.strftime("%Y-%m-%d") if dt else None,
            "start_time": getattr(v, "start_time", None),
            "end_time": getattr(v, "end_time", None),
            "duration_min": getattr(v, "duration_min", None),
            "rating": getattr(v, "rating", None),
            "memo": getattr(v, "memo", None),
            "price_items": getattr(v, "price_items", None),
            "total_yen": getattr(v, "total_yen", None),
        }

    def tpl_to_dict(t: KBPriceTemplate) -> dict:
        return {
            "id": int(getattr(t, "id")),
            "store_id": getattr(t, "store_id", None),
            "name": getattr(t, "name", None),
            "items": getattr(t, "items", None),
        }

    payload = {
        "version": 3,
        "exported_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions": [region_to_dict(r) for r in regions],
        "stores": [store_to_dict(s) for s in stores],
        "persons": [person_to_dict(p) for p in persons],
        "visits": [visit_to_dict(v) for v in visits],
        "price_templates": [tpl_to_dict(t) for t in price_templates],
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/kb/import")
def kb_import(
    request: Request,
    payload_json: str = Form(""),
    confirm_check: str = Form(""),
    mode: str = Form("replace"),  # replace のみ実装（安全寄り）
    db: Session = Depends(get_db),
):
    """
    JSONを取り込み（貼り付け想定）。
    - mode=replace: 全削除→流し込み（confirm_check=1 必須）
    - 成否は /kb?import=done|failed にリダイレクトで返す
    """
    def _redir(status: str, err: str = ""):
        q = {"import": status}
        if err:
            q["import_error"] = err
        return RedirectResponse(url="/kb?" + urlencode(q), status_code=303)

    if (confirm_check or "") != "1":
        return _redir("failed", "confirm_required")

    if mode != "replace":
        return _redir("failed", "mode_not_supported")

    raw = (payload_json or "").strip()
    if not raw:
        return _redir("failed", "payload_empty")

    # 乱用・事故防止のサイズ上限（必要なら後で増やせます）
    if len(raw.encode("utf-8")) > 5 * 1024 * 1024:
        return _redir("failed", "payload_too_large")

    try:
        data = json.loads(raw)
    except Exception:
        return _redir("failed", "invalid_json")

    if not isinstance(data, dict):
        return _redir("failed", "payload_not_object")

    regions = data.get("regions", [])
    stores = data.get("stores", [])
    persons = data.get("persons", [])
    visits = data.get("visits", [])
    price_templates = data.get("price_templates", data.get("templates", []))

    try:
        # 全削除（FK事故回避のためテンプレ→visit→person→store→region）
        db.query(KBPriceTemplate).delete(synchronize_session=False)
        db.query(KBVisit).delete(synchronize_session=False)
        db.query(KBPerson).delete(synchronize_session=False)
        db.query(KBStore).delete(synchronize_session=False)
        db.query(KBRegion).delete(synchronize_session=False)
        if _diary_state_enabled():
            try:
                db.query(KBDiaryState).delete(synchronize_session=False)  # type: ignore
            except Exception:
                pass
        db.commit()
    except Exception:
        db.rollback()
        return _redir("failed", "clear_failed")

    def _parse_utc_iso_to_dt(s: str) -> Optional[datetime]:
        try:
            t = (s or "").strip()
            if not t:
                return None
            # "YYYY-MM-DDTHH:MM:SSZ"
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    # import時に diary state を作るために一時保持
    diary_payloads: list[dict] = []

    try:
        # regions
        for r in regions if isinstance(regions, list) else []:
            if not isinstance(r, dict):
                continue
            rid = _parse_int(str(r.get("id", "")))
            name = (r.get("name", "") or "").strip()
            if rid is None or not name:
                continue
            obj = KBRegion(id=int(rid), name=name, name_norm=norm_text(name))
            db.add(obj)
        db.flush()

        # stores
        for s in stores if isinstance(stores, list) else []:
            if not isinstance(s, dict):
                continue
            sid = _parse_int(str(s.get("id", "")))
            rid = _parse_int(str(s.get("region_id", "")))
            name = (s.get("name", "") or "").strip()
            if sid is None or rid is None or not name:
                continue
            obj = KBStore(id=int(sid), region_id=int(rid), name=name, name_norm=norm_text(name))
            for k in ["area", "board_category", "board_id"]:
                if hasattr(obj, k) and k in s:
                    setattr(obj, k, s.get(k))
            db.add(obj)
        db.flush()

        # price_templates（storesが入った後）
        for t in price_templates if isinstance(price_templates, list) else []:
            if not isinstance(t, dict):
                continue
            tid = _parse_int(str(t.get("id", "")))
            sid = t.get("store_id", None)
            if sid in ("", "null"):
                sid = None
            sid_i = _parse_int(sid) if sid is not None else None

            name = _sanitize_template_name(t.get("name", ""))
            if tid is None or not name:
                continue

            items = t.get("items", None)
            if items is None:
                items_payload = None
            else:
                items_norm = _sanitize_price_template_items(items)
                items_payload = items_norm or None

            obj = KBPriceTemplate(
                id=int(tid),
                store_id=int(sid_i) if sid_i is not None else None,
                name=name,
                items=items_payload,
                created_at=datetime.utcnow() if hasattr(KBPriceTemplate, "created_at") else None,
                updated_at=datetime.utcnow() if hasattr(KBPriceTemplate, "updated_at") else None,
            )
            db.add(obj)
        db.flush()

        # persons
        person_objs: List[KBPerson] = []
        for p in persons if isinstance(persons, list) else []:
            if not isinstance(p, dict):
                continue
            pid = _parse_int(str(p.get("id", "")))
            sid = _parse_int(str(p.get("store_id", "")))
            name = (p.get("name", "") or "").strip()
            if pid is None or sid is None or not name:
                continue

            obj = KBPerson(id=int(pid), store_id=int(sid), name=name)
            obj.age = _parse_int(p.get("age", ""))
            obj.height_cm = _parse_int(p.get("height_cm", ""))
            cu = unicodedata.normalize("NFKC", str(p.get("cup", "") or "")).upper().strip()
            obj.cup = (cu[:1] if cu and "A" <= cu[:1] <= "Z" else None)
            obj.bust_cm = _parse_int(p.get("bust_cm", ""))
            obj.waist_cm = _parse_int(p.get("waist_cm", ""))
            obj.hip_cm = _parse_int(p.get("hip_cm", ""))
            obj.services = (p.get("services", "") or "").strip() or None
            obj.tags = (p.get("tags", "") or "").strip() or None
            obj.memo = (p.get("memo", "") or "").strip() or None

            if hasattr(obj, "url"):
                u = (p.get("url", "") or "").strip()
                obj.url = u or None
                if hasattr(obj, "url_norm"):
                    obj.url_norm = norm_text(obj.url or "")

            if hasattr(obj, "image_urls"):
                iu = p.get("image_urls", None)
                if isinstance(iu, list):
                    obj.image_urls = [str(x or "").strip() for x in iu if str(x or "").strip()] or None

            # diaryは後で state に入れる（KBPerson列しか無い場合はここで入る）
            diary_payloads.append(
                {
                    "person_id": int(pid),
                    "track": bool(p.get("diary_track") or False),
                    "latest_ts_ms": _parse_int(p.get("diary_latest_ts_ms", "")),
                    "seen_ts_ms": _parse_int(p.get("diary_seen_ts_ms", "")),
                    "checked_at": _parse_utc_iso_to_dt(p.get("diary_checked_at_utc", "")),
                }
            )

            obj.name_norm = norm_text(obj.name or "")
            obj.services_norm = norm_text(obj.services or "")
            obj.tags_norm = norm_text(obj.tags or "")
            obj.memo_norm = norm_text(obj.memo or "")

            db.add(obj)
            person_objs.append(obj)

        db.flush()

        # diary state insert（stateテーブルがあるならそっち優先）
        if _diary_state_enabled() and diary_payloads:
            for it in diary_payloads:
                pid = int(it.get("person_id"))
                try:
                    st = KBDiaryState(person_id=pid)  # type: ignore
                except Exception:
                    try:
                        st = KBDiaryState()  # type: ignore
                        if hasattr(st, "person_id"):
                            setattr(st, "person_id", pid)
                    except Exception:
                        continue

                # columns are optional by hasattr
                if hasattr(st, "track"):
                    setattr(st, "track", bool(it.get("track") or False))
                for k in ("latest_ts_ms", "diary_latest_ts_ms"):
                    if hasattr(st, k):
                        setattr(st, k, it.get("latest_ts_ms", None))
                        break
                for k in ("seen_ts_ms", "diary_seen_ts_ms"):
                    if hasattr(st, k):
                        setattr(st, k, it.get("seen_ts_ms", None))
                        break
                for k in ("checked_at", "diary_checked_at", "last_checked_at"):
                    if hasattr(st, k):
                        setattr(st, k, it.get("checked_at", None))
                        break
                db.add(st)

        db.flush()

        # search_norm 再生成（store/region が入った後に計算）
        for obj in person_objs:
            try:
                obj.search_norm = build_person_search_blob(db, obj)
            except Exception:
                obj.search_norm = norm_text(obj.name or "")

        # visits
        for v in visits if isinstance(visits, list) else []:
            if not isinstance(v, dict):
                continue
            vid = _parse_int(str(v.get("id", "")))
            pid = _parse_int(str(v.get("person_id", "")))
            if vid is None or pid is None:
                continue

            dt = None
            vd = (v.get("visited_at", "") or "").strip()
            if vd:
                try:
                    dt = datetime.strptime(vd, "%Y-%m-%d")
                except Exception:
                    dt = None

            # 互換：start/end は「分」でも「HH:MM」でもOKにする
            stt = _parse_minutes_or_hhmm(v.get("start_time", None))
            enn = _parse_minutes_or_hhmm(v.get("end_time", None))
            dur = _parse_minutes_or_hhmm(v.get("duration_min", None))
            if dur is None:
                dur = _calc_duration(stt, enn)

            # rating は 1..5 のみ採用
            rt = _parse_int(v.get("rating", ""))
            if rt is not None and not (1 <= int(rt) <= 5):
                rt = None

            # price_items を軽く整形（amount を整数に寄せる）
            price_items_raw = v.get("price_items", None)
            price_items_norm = None
            if isinstance(price_items_raw, list):
                items_tmp = []
                for it in price_items_raw:
                    if not isinstance(it, dict):
                        continue
                    label = str(it.get("label", "") or "").strip()
                    amt_i = _parse_amount_int(it.get("amount", 0))
                    if not label and amt_i == 0:
                        continue
                    items_tmp.append({"label": label, "amount": amt_i})
                price_items_norm = items_tmp or None

            total_yen = _parse_int(v.get("total_yen", "")) or 0
            if total_yen < 0:
                total_yen = 0
            if total_yen == 0 and isinstance(price_items_norm, list):
                try:
                    total_yen = int(sum([int(it.get("amount", 0) or 0) for it in price_items_norm]))
                except Exception:
                    total_yen = 0

            obj = KBVisit(
                id=int(vid),
                person_id=int(pid),
                visited_at=dt,
                start_time=stt,
                end_time=enn,
                duration_min=dur,
                rating=rt,
                memo=(v.get("memo", "") or "").strip() or None,
                price_items=price_items_norm if price_items_norm is not None else v.get("price_items", None),
                total_yen=int(total_yen),
            )
            try:
                obj.search_norm = build_visit_search_blob(obj)
            except Exception:
                obj.search_norm = norm_text(obj.memo or "")

            db.add(obj)

        db.flush()

        # 明示ID挿入のあと、Postgresのシーケンスを復旧
        _reset_postgres_pk_sequence(db, KBRegion)
        _reset_postgres_pk_sequence(db, KBStore)
        _reset_postgres_pk_sequence(db, KBPriceTemplate)
        _reset_postgres_pk_sequence(db, KBPerson)
        _reset_postgres_pk_sequence(db, KBVisit)
        if _diary_state_enabled():
            try:
                _reset_postgres_pk_sequence(db, KBDiaryState)  # type: ignore
            except Exception:
                pass

        db.commit()
    except Exception:
        db.rollback()
        return _redir("failed", "import_failed")

    return _redir("done")
