# 019
# routers/kb.py
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from sqlalchemy import and_, desc, exists, func, or_
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit

router = APIRouter()


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


# =========================
# KBトップ
# =========================
@router.get("/kb", response_class=HTMLResponse)
def kb_index(request: Request, db: Session = Depends(get_db)):
    regions, stores_by_region, counts = _build_tree_data(db)
    panic = request.query_params.get("panic") or ""
    search_error = request.query_params.get("search_error") or ""

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

    # 「写メ日記」も検索に含めたバージョン（⑦の軽量版としても使える）
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
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

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
        db.query(KBVisit).delete(synchronize_session=False)
        db.query(KBPerson).delete(synchronize_session=False)
        db.query(KBStore).delete(synchronize_session=False)
        db.query(KBRegion).delete(synchronize_session=False)
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
    全KBを JSON で吐き出す（移行/保険）
    """
    regions = db.query(KBRegion).order_by(KBRegion.id.asc()).all()
    stores = db.query(KBStore).order_by(KBStore.id.asc()).all()
    persons = db.query(KBPerson).order_by(KBPerson.id.asc()).all()
    visits = db.query(KBVisit).order_by(KBVisit.id.asc()).all()

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
        d = {
            "id": int(getattr(p, "id")),
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

    payload = {
        "version": 1,
        "exported_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "regions": [region_to_dict(r) for r in regions],
        "stores": [store_to_dict(s) for s in stores],
        "persons": [person_to_dict(p) for p in persons],
        "visits": [visit_to_dict(v) for v in visits],
    }
    return JSONResponse(payload)


@router.post("/kb/import")
def kb_import(
    payload_json: str = Form(""),
    confirm_check: str = Form(""),
    mode: str = Form("replace"),  # replace のみ実装（安全寄り）
    db: Session = Depends(get_db),
):
    """
    JSONを取り込み。
    - mode=replace: 全削除→流し込み（confirm_check=1 必須）
    """
    if (confirm_check or "") != "1":
        raise HTTPException(status_code=400, detail="confirm_check=1 is required")

    raw = (payload_json or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="payload_json is empty")

    try:
        data = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="payload_json is not valid JSON")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="payload_json must be an object")

    regions = data.get("regions", [])
    stores = data.get("stores", [])
    persons = data.get("persons", [])
    visits = data.get("visits", [])

    if mode != "replace":
        raise HTTPException(status_code=400, detail="only mode=replace is supported")

    try:
        # 全削除
        db.query(KBVisit).delete(synchronize_session=False)
        db.query(KBPerson).delete(synchronize_session=False)
        db.query(KBStore).delete(synchronize_session=False)
        db.query(KBRegion).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="failed to clear tables")

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

            obj.name_norm = norm_text(obj.name or "")
            obj.services_norm = norm_text(obj.services or "")
            obj.tags_norm = norm_text(obj.tags or "")
            obj.memo_norm = norm_text(obj.memo or "")

            db.add(obj)
            person_objs.append(obj)

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

            obj = KBVisit(
                id=int(vid),
                person_id=int(pid),
                visited_at=dt,
                start_time=_parse_int(v.get("start_time", "")),
                end_time=_parse_int(v.get("end_time", "")),
                duration_min=_parse_int(v.get("duration_min", "")),
                rating=_parse_int(v.get("rating", "")),
                memo=(v.get("memo", "") or "").strip() or None,
                price_items=v.get("price_items", None),
                total_yen=_parse_int(v.get("total_yen", "")) or 0,
            )
            try:
                obj.search_norm = build_visit_search_blob(obj)
            except Exception:
                obj.search_norm = norm_text(obj.memo or "")

            db.add(obj)

        db.commit()
    except HTTPException:
        raise
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="import failed")

    return JSONResponse({"ok": True})
