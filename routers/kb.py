# routers/kb.py
import json
import unicodedata
from collections import defaultdict
from datetime import datetime

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc, func, exists
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBRegion, KBStore, KBPerson, KBVisit

router = APIRouter()


# =========================
# 正規化（⑨：大文字小文字 + カタ/ひら揺らぎ対応）
# =========================

def _kata_to_hira(s: str) -> str:
    # Katakana (ァ=0x30A1 .. ヶ=0x30F6) -> Hiragana (ぁ=0x3041 .. ゖ=0x3096)
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

def build_person_search_blob(db: Session, p: KBPerson) -> str:
    store = db.query(KBStore).filter(KBStore.id == p.store_id).first()
    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first() if store else None

    parts = []
    if region and region.name:
        parts.append(region.name)
    if store and store.name:
        parts.append(store.name)

    parts.extend([
        p.name or "",
        str(p.age or ""),
        str(p.height_cm or ""),
        p.cup or "",
        str(p.bust_cm or ""),
        str(p.waist_cm or ""),
        str(p.hip_cm or ""),
        p.service or "",
        p.tags or "",
        p.memo or "",
    ])

    return norm_text(" ".join([x for x in parts if x is not None]))

def build_visit_search_blob(v: KBVisit) -> str:
    parts = [
        v.memo or "",
    ]
    if isinstance(v.price_items, list):
        for it in v.price_items:
            if isinstance(it, dict):
                parts.append(str(it.get("label", "") or ""))
                parts.append(str(it.get("amount", "") or ""))
    return norm_text(" ".join(parts))


def _parse_int(x: str):
    x = (x or "").strip()
    if not x:
        return None
    try:
        return int(x)
    except Exception:
        return None

def _parse_time_hhmm_to_min(x: str):
    x = (x or "").strip()
    if not x:
        return None
    # "HH:MM"
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
        # 日跨ぎ扱い（例: 23:30 -> 01:00）
        d += 24 * 60
    return d


# =========================
# KBトップ
# =========================

@router.get("/kb", response_class=HTMLResponse)
def kb_index(request: Request, db: Session = Depends(get_db)):
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
        db.query(KBPerson.store_id, func.count(KBPerson.id))
        .group_by(KBPerson.store_id)
        .all()
    )

    panic = request.query_params.get("panic") or ""
    search_error = request.query_params.get("search_error") or ""

    return templates.TemplateResponse(
        "kb_index.html",
        {
            "request": request,
            "regions": regions,
            "stores_by_region": stores_by_region,
            "person_counts": counts,
            "panic": panic,
            "search_error": search_error,
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
            db.add(KBRegion(name=name))
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
            db.add(KBStore(region_id=int(region_id), name=name))
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

    return templates.TemplateResponse(
        "kb_store.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "persons": persons,
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
        exists = (
            db.query(KBPerson)
            .filter(KBPerson.store_id == int(store_id), KBPerson.name == name)
            .first()
        )
        if exists:
            return RedirectResponse(url=f"/kb/person/{exists.id}", status_code=303)

        p = KBPerson(store_id=int(store_id), name=name)
        db.add(p)
        db.commit()
        db.refresh(p)

        # search_norm 初期化
        try:
            p.search_norm = build_person_search_blob(db, p)
            db.commit()
        except Exception:
            db.rollback()

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

    return templates.TemplateResponse(
        "kb_person.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "person": person,
            "visits": visits,
            "rating_avg": rating_avg,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
        },
    )


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
    service: str = Form(""),
    tags: str = Form(""),
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
        p.cup = (cup or "").strip() or None
        p.bust_cm = _parse_int(bust_cm)
        p.waist_cm = _parse_int(waist_cm)
        p.hip_cm = _parse_int(hip_cm)
        p.service = (service or "").strip() or None
        p.tags = (tags or "").strip() or None
        p.memo = (memo or "").strip() or None

        p.search_norm = build_person_search_blob(db, p)
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/kb/person/{person_id}/visit")
def kb_add_visit(
    request: Request,
    person_id: int,
    visited_date: str = Form(""),       # YYYY-MM-DD
    start_time: str = Form(""),         # HH:MM
    end_time: str = Form(""),           # HH:MM
    rating: str = Form(""),             # 1-5
    memo: str = Form(""),
    price_items_json: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    # visited_date -> DateTime（00:00）で保持（表示はテンプレで日付だけにする）
    dt = None
    vd = (visited_date or "").strip()
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
                    label = str(it.get("label", "")).strip()
                    amt = it.get("amount", 0)
                    try:
                        amt_i = int(amt)
                    except Exception:
                        amt_i = 0
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
            start_min=smin,
            end_min=emin,
            duration_min=dur,
            rating=r,
            memo=(memo or "").strip() or None,
            price_items=items or None,
            total_yen=total if items else (total if total else None),
        )
        v.search_norm = build_visit_search_blob(v)
        db.add(v)
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


# =========================
# ⑦ 人物削除（利用ログも削除）
# =========================

@router.post("/kb/person/{person_id}/delete")
def kb_delete_person(request: Request, person_id: int, db: Session = Depends(get_db)):
    back_url = request.headers.get("referer") or "/kb"
    try:
        # visits -> person の順
        db.query(KBVisit).filter(KBVisit.person_id == int(person_id)).delete(synchronize_session=False)
        db.query(KBPerson).filter(KBPerson.id == int(person_id)).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)


# =========================
# ⑧ パニック全削除
# =========================

@router.post("/kb/panic_delete_all")
def kb_panic_delete_all(
    request: Request,
    confirm_text: str = Form(""),
    confirm_check: str = Form(""),
    db: Session = Depends(get_db),
):
    # 確認は絶対（2段階）
    if (confirm_text or "").strip() != "DELETE" or (confirm_check or "") != "1":
        return RedirectResponse(url="/kb?panic=failed", status_code=303)

    try:
        # 参照順を考慮して下から消す
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
# ⑨ KB フリーワード検索（地域で絞り込み可）
# =========================

@router.get("/kb/search", response_class=HTMLResponse)
def kb_search(request: Request, q: str = "", region_id: str = "", db: Session = Depends(get_db)):
    q_raw = (q or "").strip()
    if not q_raw:
        return RedirectResponse(url="/kb?search_error=empty", status_code=303)

    qn = norm_text(q_raw)

    regions = db.query(KBRegion).order_by(KBRegion.name.asc()).all()

    # region filter
    rid = _parse_int(region_id)

    # person.search_norm contains q OR exists visit.search_norm contains q
    person_q = db.query(KBPerson)

    if rid:
        person_q = (
            person_q.join(KBStore, KBStore.id == KBPerson.store_id)
                    .join(KBRegion, KBRegion.id == KBStore.region_id)
                    .filter(KBRegion.id == rid)
        )

    visit_match = (
        db.query(KBVisit.id)
        .filter(KBVisit.person_id == KBPerson.id, KBVisit.search_norm.isnot(None), KBVisit.search_norm.contains(qn))
    )

    person_q = person_q.filter(
        (KBPerson.search_norm.isnot(None) & KBPerson.search_norm.contains(qn))
        | exists(visit_match)
    )

    persons = (
        person_q.order_by(KBPerson.name.asc())
                .limit(300)
                .all()
    )

    # store/regionを出すためにまとめて引く（N+1回避）
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

    return templates.TemplateResponse(
        "kb_index.html",
        {
            "request": request,
            "regions": regions,
            "stores_by_region": defaultdict(list),  # 通常表示は下で別に出すので空でOK
            "person_counts": {},
            "panic": request.query_params.get("panic") or "",
            "search_error": "",
            "search_q": q_raw,
            "search_region_id": rid or "",
            "search_results": persons,
            "stores_map": stores,
            "regions_map": regions_map,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
        },
    )
