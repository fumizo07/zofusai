# 001
# routers/kb_parts/pages.py
from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import and_, desc, exists, func, or_
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit, KBPriceTemplate

from .diary_core import (
    diary_state_enabled,
    get_diary_state_map,
    get_or_create_diary_state,
    get_person_diary_track,
    set_person_diary_track,
    set_person_diary_checked_at,
)
from .utils import (
    SORT_OPTIONS,
    avg_amount_map_for_person_ids,
    avg_rating_map_for_person_ids,
    build_google_search_url,
    build_google_site_search_url,
    build_person_search_blob,
    build_store_region_maps,
    build_tree_data,
    build_visit_search_blob,
    calc_duration,
    collect_service_tag_options,
    cup_bucket_hit,
    cup_letter,
    filter_persons_by_rating_min,
    find_similar_persons_in_store,
    last_visit_map_for_person_ids,
    make_store_keyword,
    norm_text,
    normalize_sort_params,
    parse_amount_int,
    parse_int,
    parse_rating_min,
    parse_time_hhmm_to_min,
    sanitize_image_urls,
    sort_persons,
    token_set_norm,
)


router = APIRouter()


@router.get("/kb", response_class=HTMLResponse)
def kb_index(request: Request, db: Session = Depends(get_db)):
    regions, stores_by_region, counts = build_tree_data(db)
    panic = request.query_params.get("panic") or ""
    search_error = request.query_params.get("search_error") or ""
    import_status = request.query_params.get("import") or ""
    import_error = request.query_params.get("import_error") or ""

    svc_options, tag_options = collect_service_tag_options(db)

    sort_eff, order_eff = normalize_sort_params("name", "asc")

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
    rating_avg_map = avg_rating_map_for_person_ids(db, person_ids)
    amount_avg_map = avg_amount_map_for_person_ids(db, person_ids)
    last_visit_map = last_visit_map_for_person_ids(db, person_ids)

    sort_eff, order_eff = normalize_sort_params(sort, order)

    rmin = parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

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
            dup = find_similar_persons_in_store(
                db, int(store_id), name, exclude_person_id=int(exists_p.id)
            )
            dup_ids = ",".join([str(int(x.id)) for x in dup if x and getattr(x, "id", None)])
            url = f"/kb/person/{exists_p.id}"
            if dup_ids:
                url += "?dup=" + dup_ids
            return RedirectResponse(url=url, status_code=303)

        dup = find_similar_persons_in_store(db, int(store_id), name, exclude_person_id=None)

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

    base_parts = []
    if region and region.name:
        base_parts.append(region.name)
    if store and store.name:
        base_parts.append(store.name)
    if person and person.name:
        base_parts.append(person.name)
    base_q = " ".join([x for x in base_parts if x]).strip()

    google_all_url = build_google_search_url(base_q)
    google_cityheaven_url = build_google_site_search_url("cityheaven.net", base_q)
    google_dto_url = build_google_site_search_url("dto.jp", base_q)

    diary_q = (base_q + " 写メ日記").strip() if base_q else ""
    google_all_diary_url = build_google_search_url(diary_q)
    google_cityheaven_diary_url = build_google_site_search_url("cityheaven.net", diary_q)
    google_dto_diary_url = build_google_site_search_url("dto.jp", diary_q)

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


@router.get("/kb/person/{person_id}/external_search")
def kb_person_external_search(person_id: int, db: Session = Depends(get_db)):
    person = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not person:
        return RedirectResponse(url="/kb", status_code=303)

    store = db.query(KBStore).filter(KBStore.id == person.store_id).first()

    store_name = (store.name or "").strip() if store else ""
    person_name = (person.name or "").strip()

    store_kw = make_store_keyword(store_name)
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
    diary_track: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    state_map = get_diary_state_map(db, [int(person_id)])
    st = state_map.get(int(person_id))
    if diary_state_enabled():
        st = get_or_create_diary_state(db, state_map, int(person_id)) or st

    try:
        p.name = (name or "").strip() or p.name
        p.age = parse_int(age)
        p.height_cm = parse_int(height_cm)

        cu = (cup or "").strip()
        cu = unicodedata.normalize("NFKC", cu).upper()
        p.cup = (cu[:1] if cu and "A" <= cu[:1] <= "Z" else None)

        p.bust_cm = parse_int(bust_cm)
        p.waist_cm = parse_int(waist_cm)
        p.hip_cm = parse_int(hip_cm)

        p.services = (services or "").strip() or None
        p.tags = (tags or "").strip() or None

        if hasattr(p, "url"):
            u = (url or "").strip()
            p.url = u or None
            if hasattr(p, "url_norm"):
                p.url_norm = norm_text(p.url or "")

        if hasattr(p, "image_urls"):
            urls = sanitize_image_urls(image_urls_text or "")
            p.image_urls = urls or None

        p.memo = (memo or "").strip() or None

        new_track = (diary_track or "").strip().lower() in ("1", "true", "on", "yes")
        old_track = get_person_diary_track(p, st)

        if set_person_diary_track(p, new_track, st):
            if (not old_track) and new_track:
                set_person_diary_checked_at(p, None, st)

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

    smin = parse_time_hhmm_to_min(start_time)
    emin = parse_time_hhmm_to_min(end_time)
    dur = calc_duration(smin, emin)

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
                    amt_i = parse_amount_int(amt)

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

    smin = parse_time_hhmm_to_min(start_time)
    emin = parse_time_hhmm_to_min(end_time)
    dur = calc_duration(smin, emin)

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
                    amt_i = parse_amount_int(amt)

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
        if diary_state_enabled():
            try:
                from models import KBDiaryState  # type: ignore
                db.query(KBDiaryState).filter(KBDiaryState.person_id == int(person_id)).delete(synchronize_session=False)  # type: ignore
            except Exception:
                pass
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)


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
    sort: str = "name",
    order: str = "",
    rating_min: str = "",
    star_only: str = "",
):
    regions, stores_by_region, counts = build_tree_data(db)
    svc_options, tag_options = collect_service_tag_options(db)

    rid = parse_int(region_id)
    bmin = parse_int(budget_min)
    bmax = parse_int(budget_max)

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
        ps = token_set_norm(getattr(p, "services", None))
        return any(x in ps for x in svc_norm_set)

    def hit_tag(p: KBPerson) -> bool:
        if not tag_norm_set:
            return True
        pt = token_set_norm(getattr(p, "tags", None))
        return any(x in pt for x in tag_norm_set)

    def hit_cup(p: KBPerson) -> bool:
        if not cup:
            return True
        c = cup_letter(getattr(p, "cup", None))
        return any(cup_bucket_hit(b, c) for b in cup)

    persons = [p for p in candidates if hit_svc(p) and hit_tag(p) and hit_cup(p)]

    truncated = False
    total_count = len(persons)
    if len(persons) > 500:
        persons = persons[:500]
        truncated = True

    stores_map, regions_map = build_store_region_maps(db, persons)
    rating_avg_map = avg_rating_map_for_person_ids(db, [p.id for p in persons])
    amount_avg_map = avg_amount_map_for_person_ids(db, [p.id for p in persons])
    last_visit_map = last_visit_map_for_person_ids(db, [p.id for p in persons])

    sort_eff, order_eff = normalize_sort_params(sort, order)

    rmin = parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

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
            "sort_options": SORT_OPTIONS,
            "sort": sort_eff,
            "order": order_eff,
            "rating_min": str(rmin) if rmin is not None else "",
            "star_only": "1" if (star_only or "") == "1" else "",
        },
    )
