# 001
# routers/kb_parts/utils.py
from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from sqlalchemy import and_, exists, func, or_, text
from sqlalchemy.orm import Session

from models import KBPerson, KBRegion, KBStore, KBVisit, KBPriceTemplate


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
# parse系
# =========================
def parse_int(x: object) -> Optional[int]:
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


def parse_amount_int(x: object) -> int:
    v = parse_int(x)
    if v is None:
        return 0
    if v < 0:
        return 0
    return int(v)


def parse_time_hhmm_to_min(x: str) -> Optional[int]:
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


def parse_minutes_or_hhmm(x: object) -> Optional[int]:
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
        v = parse_time_hhmm_to_min(s)
        if v is None:
            return None
        return int(v)
    v = parse_int(s)
    if v is None or v < 0:
        return None
    return int(v)


def calc_duration(start_min: Optional[int], end_min: Optional[int]) -> Optional[int]:
    if start_min is None or end_min is None:
        return None
    d = end_min - start_min
    if d < 0:
        d += 24 * 60
    return d


# =========================
# 外部検索用：店名→タイトル検索keyword生成（〇〇店/住所っぽい塊対策）
# =========================
_STORE_STOPWORDS = {
    "club", "bar", "salon", "spa", "lounge", "room", "healing", "the", "and",
    "massage", "aroma", "esthetic", "esthe", "relax", "relaxation",
    "クラブ", "くらぶ", "バー", "さろん", "サロン", "スパ", "ラウンジ", "ルーム",
    "メンズ", "メンエス", "エステ", "マッサージ", "アロマ", "癒し",
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


def make_store_keyword(store_name: str) -> str:
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


# =========================
# 雑多ユーティリティ
# =========================
def sanitize_image_urls(raw: str) -> list[str]:
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


def token_set_norm(raw: Optional[str]) -> set[str]:
    toks = _split_tokens(raw)
    return {norm_text(t) for t in toks if t}


def collect_service_tag_options(db: Session) -> Tuple[List[str], List[str]]:
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


def cup_letter(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = unicodedata.normalize("NFKC", raw).upper()
    m = re.search(r"[A-Z]", s)
    return m.group(0) if m else ""


def cup_bucket_hit(bucket: str, cup_l: str) -> bool:
    if not cup_l:
        return False
    c = cup_l
    if bucket == "leD":
        return "A" <= c <= "D"
    if bucket == "EF":
        return c in {"E", "F"}
    if bucket == "geG":
        return "G" <= c <= "Z"
    return False


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


# =========================
# search_norm生成
# =========================
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


def normalize_sort_params(sort_key: str, order: str) -> tuple[str, str]:
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


def parse_rating_min(x: str) -> Optional[int]:
    v = parse_int(x)
    if v is None:
        return None
    if 1 <= v <= 5:
        return int(v)
    return None


def last_visit_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, datetime]:
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


def avg_rating_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, float]:
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
    out: dict[int, float] = {}
    for pid, avg in rows:
        if pid is None or avg is None:
            continue
        out[int(pid)] = float(avg)
    return out


def avg_amount_map_for_person_ids(db: Session, person_ids: list[int]) -> dict[int, int]:
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


def filter_persons_by_rating_min(
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


def sort_persons(
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

    if od not in ("asc", "desc"):
        od = "asc" if sk == "name" else "desc"

    if sk == "name":
        return sorted(
            persons,
            key=lambda p: norm_text(getattr(p, "name", "") or ""),
            reverse=(od == "desc"),
        )

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
        nm = norm_text(getattr(p, "name", "") or "")
        return (missing, vv, nm)

    return sorted(persons, key=key_fn)


def find_similar_persons_in_store(
    db: Session,
    store_id: int,
    name: str,
    exclude_person_id: Optional[int] = None,
    limit: int = 5,
) -> List[KBPerson]:
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

        if pn == n:
            continue

        contains_hit = (n in pn) or (pn in n)

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
# 価格テンプレ helpers
# =========================
def sanitize_template_name(raw: str) -> str:
    s = unicodedata.normalize("NFKC", str(raw or "")).strip()
    s = re.sub(r"\s+", " ", s)
    if len(s) > 60:
        s = s[:60].strip()
    return s


def sanitize_price_template_items(items: object) -> list[dict]:
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
        amt = parse_amount_int(amt_raw)

        if not label and amt == 0:
            continue

        out.append({"label": label, "amount": int(amt)})

        if len(out) >= 40:
            break

    return out


def utc_iso(dt: object) -> Optional[str]:
    if not dt:
        return None
    try:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def reset_postgres_pk_sequence(db: Session, model) -> None:
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
# KBツリー生成
# =========================
def build_tree_data(db: Session):
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


def build_store_region_maps(db: Session, persons: List[KBPerson]):
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
