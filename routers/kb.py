# 017
# routers/kb.py
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import and_, desc, exists, func, or_
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit

router = APIRouter()


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

# stopwordの正規化済み集合（都度生成しない）
_STORE_STOPWORDS_NORM = {norm_store_kw(x) for x in _STORE_STOPWORDS}

_STORE_SEP_RE = re.compile(r"[ \t\r\n\u3000\(\)\[\]{}（）【】「」『』<>/\\|・\-–—_.,!?:;]+")
_STORE_ALNUM_RE = re.compile(r"^[0-9a-z]+$", re.IGNORECASE)
_STORE_ALPHA_RE = re.compile(r"^[a-z]+$", re.IGNORECASE)
_STORE_DIGITS_RE = re.compile(r"^\d+$")

# 末尾が「〜店」を “絶対に” keyword に入れないための判定用（よくある派生も含める）
_STORE_BRANCH_TAIL_RE = re.compile(r"(本店|支店|別館|新店|[0-9]+号店|店)$")
# 住所っぽい（ビル/階/号など）が混ざった塊を避ける（完全禁止ではなく減点）
_STORE_ADDR_HINT_RE = re.compile(r"(ビル|びる|階|f|号|丁目|番地|通り|駅前|東口|西口|南口|北口)")

# 「堺店」「梅田店」など “支店ブロックだけ” のトークンを捨てる
# （※カタカナは含めない：サマンサ店 のようなブランドを誤って捨てないため）
_STORE_PURE_BRANCH_RE = re.compile(r"^[\u3041-\u309f\u4e00-\u9fff]{1,8}(本店|支店|別館|新店|[0-9]+号店|店)$")

# 「サマンサ堺店」など、連結トークンの末尾に「<地名><店/本店...>」が付く場合は
# その “支店ブロック（地名+店）” を丸ごと落としてブランドだけ残す
_STORE_BRANCH_LOC_TAIL_RE = re.compile(
    r"^(.+?)([\u3041-\u309f\u4e00-\u9fff]{1,8})(本店|支店|別館|新店|[0-9]+号店|店)$"
)


def _tokenize_store_name(raw: str) -> List[str]:
    """
    店名を「それっぽい単語」に分割する
    - NFKC + lower（※カタ→ひらはしない）
    - 記号類はスペース
    - 連続スペースを潰す
    """
    s = norm_store_kw(raw or "")
    s = _STORE_SEP_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return []
    return [t for t in s.split(" ") if t]


def _is_stopword(tok_norm: str) -> bool:
    return (tok_norm or "") in _STORE_STOPWORDS_NORM


def _variants(tok_norm: str) -> List[str]:
    """
    トークンから候補バリアントを作る

    目的：
    - 「〇〇店」だけでなく「堺店」「梅田店」など支店ブロックは丸ごと落としたい
    - さらに外部検索keywordでは、カタ→ひら変換はしない

    方針：
    - トークン自体が「堺店」「梅田店」等（支店ブロックのみ）なら “捨てる”
    - トークンが「サマンサ堺店」等（ブランド+支店ブロック連結）なら
      “支店ブロック（堺店）” を丸ごと落として「サマンサ」だけ候補にする
    - それ以外は従来通り末尾の「店系」を剥がす
    """
    out: List[str] = []
    t = (tok_norm or "").strip()
    if not t:
        return out

    # 支店ブロックだけのトークンは捨てる（例：堺店 / 梅田店）
    if _STORE_PURE_BRANCH_RE.match(t):
        return out

    # 連結トークンの末尾が「<地名><店/本店...>」なら、そのブロックを丸ごと落としてブランドだけ
    m = _STORE_BRANCH_LOC_TAIL_RE.match(t)
    if m:
        brand = (m.group(1) or "").strip()
        if brand:
            out.append(brand)
        # ここで返す：わざと「サマンサ堺」など “地名だけ残る形” を作らない
        return out

    # 通常ケース：元トークンも候補に入れる
    out.append(t)

    # 末尾の「本店/支店/◯号店/店」を剥がす
    t2 = _STORE_BRANCH_TAIL_RE.sub("", t).strip()
    if t2 and t2 != t:
        out.append(t2)

    # 末尾の「ビル/ビルディング/タワー/プラザ/センター」を剥がす（住所塊の弱体化）
    for suf in ["ビルディング", "ビル", "びる", "タワー", "プラザ", "センター"]:
        if t2.endswith(norm_store_kw(suf)):
            t3 = t2[: -len(norm_store_kw(suf))].strip()
            if t3:
                out.append(t3)

    # 重複除去（順序維持）
    seen = set()
    uniq = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _score_keyword_candidate(tok_norm: str) -> int:
    """
    大きいほど強い候補
    基本方針：
      - 英字ブランド（blenda など）を最優先
      - 末尾が「〜店」は *絶対に採用しない*（ここに来る前に弾く想定だが保険で0点）
      - 住所っぽい塊（ビル/階/号/丁目/番地…）は強く減点
      - 数字混じりも減点（谷9 みたいなやつ）
    """
    t = (tok_norm or "").strip()
    if not t:
        return -10

    # 「〇〇店」を完全排除
    if _STORE_BRANCH_TAIL_RE.search(t):
        return -999

    # stopwordは基本捨てる
    if _is_stopword(t):
        return -500

    # 英字だけ（ブランド想定）は最強
    if _STORE_ALPHA_RE.match(t) and 3 <= len(t) <= 14:
        return 1000 + len(t)

    # 英数字混在（例: blenda2 など）は次点
    if _STORE_ALNUM_RE.match(t):
        if any("a" <= ch.lower() <= "z" for ch in t) and 3 <= len(t) <= 16:
            # 数字だけは別扱い（弱い）
            if _STORE_DIGITS_RE.match(t):
                return -200
            return 850 + len(t)
        # 短い英数字はノイズ
        return -50

    # 日本語系：長すぎる塊はノイズになりがち
    base = 300

    # 住所ヒントがあるなら大減点
    if _STORE_ADDR_HINT_RE.search(t):
        base -= 180

    # 数字が入るなら減点
    if any("0" <= ch <= "9" for ch in t):
        base -= 120

    # 長さは 2〜6 が最も扱いやすい
    L = len(t)
    if 2 <= L <= 6:
        base += 120 + L * 8
    elif 7 <= L <= 10:
        base += 40
    else:
        base -= 30  # 長すぎ

    return base


def _make_store_keyword(store_name: str) -> str:
    """
    タイトル検索keywordを作る（安定版）
    優先：
      1) 英字ブランド（例: blenda）
      2) 英数字ブランド（例: blenda2）
      3) 日本語の“固有語っぽい短いトークン”（住所塊は避ける）

    重要：
      - 「〇〇店」は絶対に採用しない
      - 「堺店」「梅田店」など “支店ブロック” は丸ごと捨て、
        「サマンサ堺店」→「サマンサ」のようにブランドだけ残す
      - 外部検索keywordでは、カタ→ひら変換はしない
    """
    raw = (store_name or "").strip()
    if not raw:
        return ""

    toks = _tokenize_store_name(raw)
    if not toks:
        return ""

    candidates: List[str] = []
    for t in toks:
        # まず stopword 自体は捨てる（club等）
        if _is_stopword(t):
            continue

        # バリアント生成（店/ビル等を剥がした候補も作る）
        for v in _variants(t):
            v = (v or "").strip()
            if not v:
                continue
            # 「〇〇店」を完全排除
            if _STORE_BRANCH_TAIL_RE.search(v):
                continue
            # stopwordは除外
            if _is_stopword(v):
                continue
            candidates.append(v)

    if not candidates:
        # 最後の保険：店名そのものから強引に（ただし店末尾は削る）
        s = norm_store_kw(raw)
        s = _STORE_BRANCH_TAIL_RE.sub("", s).strip()
        s = s[:8] if len(s) >= 8 else s
        return s

    # スコア最大を採用
    best = None
    best_score = -10**9
    for c in candidates:
        sc = _score_keyword_candidate(c)
        if sc > best_score:
            best_score = sc
            best = c

    # さらに保険：短すぎたら（1文字等）切り捨て
    if not best or len(best) <= 1:
        # ここに来るのは稀。無難に先頭6
        s = norm_store_kw(raw)
        s = _STORE_BRANCH_TAIL_RE.sub("", s).strip()
        return s[:6] if len(s) > 6 else s

    return best


def _sanitize_image_urls(raw: str) -> list[str]:
    """
    テキスト入力（複数行）→ URLリストへ
    - http(s)以外は捨てる（javascript: 等を避ける）
    - 空行除外
    - 重複除外（順序維持）
    - 件数上限（暴走防止）
    """
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
    """
    services/tags の「カンマ区切り想定」をトークン配列にする
    - 区切り: , ， 、 / 改行 / タブ
    - 前後空白除去
    - 空要素除外
    """
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
    """
    トークン集合（正規化済）を作る
    """
    toks = _split_tokens(raw)
    return {norm_text(t) for t in toks if t}


def _collect_service_tag_options(db: Session) -> Tuple[List[str], List[str]]:
    """
    全人物から services/tags を集計してチェックボックスの候補にする
    - 表示文字列は「最初に見つかった表記」を採用
    - 並びは表示文字列の昇順
    """
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
    """
    cup欄から A-Z の1文字を抜く
    - NFKC + upper
    - 先に出た英字を採用（例：'Ｄカップ'→'D'）
    """
    if not raw:
        return ""
    s = unicodedata.normalize("NFKC", raw).upper()
    m = re.search(r"[A-Z]", s)
    return m.group(0) if m else ""


def _cup_bucket_hit(bucket: str, cup_letter: str) -> bool:
    """
    cup_bucket:
      - leD: A-D
      - EF: E,F
      - geG: G-Z
    """
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
    """
    int化（ゆるめ）
    - NFKC
    - カンマ/空白/通貨記号/「円」などを除去
    - 文字列中の最初の -?\d+ を採用
    """
    s = unicodedata.normalize("NFKC", str(x or "")).strip()
    if not s:
        return None

    s = s.replace(",", "")
    s = s.replace("_", "")
    s = re.sub(r"[ \t\u3000]", "", s)  # 半角/全角スペース
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
    """
    金額用：カンマ有無などを許容して int にする
    - 失敗は 0
    - マイナスは 0 に丸め（防御）
    """
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
    """
    person_id ごとの平均金額（total_yen の平均）を返す
    - total_yen が None は除外
    - total_yen <= 0 は除外（=0円ログが平均を汚染しない）
    - 返り値は「円の整数」（四捨五入）
    """
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
            "svc_options": svc_options,
            "tag_options": tag_options,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
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
def kb_store_page(request: Request, store_id: int, db: Session = Depends(get_db)):
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

    rating_avg_map = _avg_rating_map_for_person_ids(db, [p.id for p in persons])
    amount_avg_map = _avg_amount_map_for_person_ids(db, [p.id for p in persons])

    return templates.TemplateResponse(
        "kb_store.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "persons": persons,
            "rating_avg_map": rating_avg_map,
            "amount_avg_map": amount_avg_map,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
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
            return RedirectResponse(url=f"/kb/person/{exists_p.id}", status_code=303)

        p = KBPerson(store_id=int(store_id), name=name)
        p.name_norm = norm_text(name)
        p.search_norm = build_person_search_blob(db, p)

        db.add(p)
        db.commit()
        db.refresh(p)
        return RedirectResponse(url=f"/kb/person/{p.id}", status_code=303)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


# =========================
# 人物ページ
# =========================
@router.get("/kb/person/{person_id}", response_class=HTMLResponse)
def kb_person_page(request: Request, person_id: int, db: Session = Depends(get_db)):
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

    # タイトル検索（keyword）は「〇〇店」を排除し、ブランド優先で作る
    store_kw = _make_store_keyword(store_name)
    params = {"keyword": store_kw}

    # KB経由は履歴に積まない（外部検索ページからの検索だけ履歴に残す）
    params["no_log"] = "1"
    # KB経由フラグ（板フォールバック等をKB時だけに限定するため）
    params["kb"] = "1"

    # 本文キーワード用（thread_search.html 側で post_kw を value に入れる前提）
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

        # URL
        if hasattr(p, "url"):
            u = (url or "").strip()
            p.url = u or None
            if hasattr(p, "url_norm"):
                p.url_norm = norm_text(p.url or "")

        # 画像URL（複数）
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
    visited_at: str = Form(""),  # YYYY-MM-DD
    start_time: str = Form(""),  # HH:MM
    end_time: str = Form(""),  # HH:MM
    rating: str = Form(""),  # 1-5
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


# =========================
# 利用ログ編集（追加）
# =========================
@router.post("/kb/visit/{visit_id}/update")
def kb_update_visit(
    request: Request,
    visit_id: int,
    visited_at: str = Form(""),  # YYYY-MM-DD
    start_time: str = Form(""),  # HH:MM
    end_time: str = Form(""),  # HH:MM
    rating: str = Form(""),  # 1-5
    memo: str = Form(""),
    price_items_json: str = Form(""),  # 空=変更なし, "[]"=クリア, JSON=上書き
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
#  - 何も指定せず検索 → 全員表示
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
            "svc_options": svc_options,
            "tag_options": tag_options,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
        },
    )
