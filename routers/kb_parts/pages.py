# 007
# routers/kb_parts/pages.py
from __future__ import annotations

import os
import json
import unicodedata
from datetime import datetime
from typing import List, Tuple, Optional
from urllib.parse import urlencode, urlparse, urlunparse

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import and_, desc, exists, func, or_
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit

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

def _dt_to_epoch_ms(dt) -> int | None:
    """datetime -> epoch(ms)。dtがNoneならNone。"""
    if not dt:
        return None
    try:
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _get_kb_allow_secret() -> str:
    return (os.getenv("KB_ALLOW_SECRET", "") or "").strip()


def _coerce_bool(v) -> bool:
    """
    "0"/"1" の文字列や int を正しく bool に寄せる。
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
    return False

# =========================
# 出勤用ヘルパー
# =========================
def _normalize_work_start(v: str | None) -> str | None:
    s = (v or "").strip()
    if not s:
        return None
    if s == "early":
        return "early"
    if s == "late":
        return "late"
    if len(s) == 5 and s[2] == ":":
        hh = parse_int(s[:2])
        mm = parse_int(s[3:])
        if hh is not None and mm in (0, 30):
            if (hh > 10 or (hh == 10 and mm >= 0)) and (hh < 24 or (hh == 24 and mm <= 30)):
                return f"{int(hh):02d}:{int(mm):02d}"
    return None


def _work_start_sort_key(v: str | None) -> int:
    s = _normalize_work_start(v)
    if s == "early":
        return -1
    if s == "late":
        return 99999
    if s and len(s) == 5 and s[2] == ":":
        hh = int(s[:2])
        mm = int(s[3:])
        return hh * 60 + mm
    return 88888


def _work_start_label(v: str | None) -> str:
    s = _normalize_work_start(v)
    if s == "early":
        return "早朝(10時前)"
    if s == "late":
        return "深夜(25時以降)"
    return s or ""

# =========================
# DTO URL: fetchは www / clickは s を使い分け
# =========================
def _is_dto_host(host: str) -> bool:
    h = (host or "").strip().lower()
    return h in ("dto.jp", "www.dto.jp", "s.dto.jp")


def _normalize_url_https(raw: str) -> str:
    """
    スキーム無しURLを https として解釈。
    解析不能なら空文字。
    """
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        if s.startswith(("http://", "https://")):
            u = urlparse(s)
        else:
            # //example.com/... も拾う
            if s.startswith("//"):
                u = urlparse("https:" + s)
            else:
                u = urlparse("https://" + s)
    except Exception:
        return ""
    if not u.netloc:
        return ""
    scheme = "https"
    return urlunparse((scheme, u.netloc, u.path or "", u.params or "", u.query or "", u.fragment or ""))


def _coerce_dto_hosts(fetch_url: str) -> Tuple[str, str]:
    """
    返り値: (fetch_www_url, open_sp_url)
    - 埋め込み(data-diary-url)は www.dto.jp に統一（Userscript/解析用）
    - クリック遷移(open)は dto.jp に統一
    dto系以外は (fetch_url, fetch_url) を返す。
    """
    u0 = _normalize_url_https(fetch_url)
    if not u0:
        return "", ""
    u = urlparse(u0)

    host = (u.netloc or "").lower()
    if not _is_dto_host(host):
        return u0, u0

    # hostだけ差し替え（path/queryは保持）
    fetch_host = "www.dto.jp"
    open_host = "dto.jp"

    fetch_www = urlunparse(("https", fetch_host, u.path or "", u.params or "", u.query or "", u.fragment or ""))
    open_sp = urlunparse(("https", open_host, u.path or "", u.params or "", u.query or "", u.fragment or ""))
    return fetch_www, open_sp


def _normalize_url_for_dup(raw: str) -> str:
    """
    重複検出用のURL正規化キーを作る。
    - スキームは https 扱い（_normalize_url_https を使う）
    - host は小文字、www. は除去
    - dto系は click側に寄せる（www/s の揺れ吸収）
    - query/fragment は捨てる（追跡パラメータ等の揺れを吸収）
    - path 末尾の / は除去
    """
    u0 = _normalize_url_https(raw)
    if not u0:
        return ""

    # dto系は open(host=dto.jp) に寄せて揺れ吸収
    fetch_url, open_url = _coerce_dto_hosts(u0)
    u = urlparse(open_url or u0)

    host = (u.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]

    path = (u.path or "").strip()
    # 末尾スラッシュは削る（/diary/ と /diary の揺れ等）
    path = path.rstrip("/")

    if not host:
        return ""

    # query/fragment は捨てる
    key = f"{host}{path}"
    return key



def _attach_diary_urls_for_templates(persons: List[KBPerson]) -> Tuple[dict[int, str], dict[int, str]]:
    """
    persons の各要素に、テンプレで使えるよう下記を付与します。
      - person.kb_diary_fetch_url : data-diary-url に入れる想定（dtoはwwwへ矯正）
      - person.kb_diary_open_url  : クリック遷移URL（dtoはsへ矯正）

    併せて map も返す（テンプレが map 参照でも使える）
      - fetch_map[person_id] = fetch_url
      - open_map[person_id]  = open_url
    """
    fetch_map: dict[int, str] = {}
    open_map: dict[int, str] = {}

    for p in persons or []:
        try:
            pid = int(getattr(p, "id", 0) or 0)
        except Exception:
            pid = 0
        if pid <= 0:
            continue

        raw = ""
        if hasattr(p, "url"):
            try:
                raw = (getattr(p, "url", "") or "").strip()
            except Exception:
                raw = ""
        if not raw:
            # URL未設定なら空のまま
            try:
                setattr(p, "kb_diary_fetch_url", "")
                setattr(p, "kb_diary_open_url", "")
            except Exception:
                pass
            continue

        fetch_url, open_url = _coerce_dto_hosts(raw)

        fetch_map[pid] = fetch_url
        open_map[pid] = open_url

        try:
            setattr(p, "kb_diary_fetch_url", fetch_url)
            setattr(p, "kb_diary_open_url", open_url)
        except Exception:
            pass

    return fetch_map, open_map


def _pick_attr(obj, candidates: List[str]) -> str:
    for a in candidates:
        if obj is not None and hasattr(obj, a):
            return a
    return ""


def _read_track(person: KBPerson, st) -> bool:
    """
    追跡フラグを「どこに保存されていても」読めるようにする。
    優先順：diary_core(get_person_diary_track) → State → Person
    """
    # まずプロジェクト既存の読み取りロジックを優先
    try:
        return _coerce_bool(get_person_diary_track(person, st))
    except Exception:
        pass

    # State側フォールバック
    if st is not None:
        a = _pick_attr(st, ["track_diary", "diary_track", "is_tracking", "tracking", "track"])
        if a:
            try:
                return _coerce_bool(getattr(st, a))
            except Exception:
                pass

    # Person側フォールバック
    a = _pick_attr(person, ["track_diary", "diary_track", "is_tracking", "tracking"])
    if a:
        try:
            return _coerce_bool(getattr(person, a))
        except Exception:
            pass

    return False


def _write_track_value(obj, attr: str, v: bool) -> None:
    """
    カラム型が str の場合は "0"/"1" にする。
    bool/int の場合は bool を入れる。
    """
    if not obj or not attr:
        return
    try:
        cur = getattr(obj, attr, None)
        if isinstance(cur, str):
            setattr(obj, attr, "1" if v else "0")
        else:
            setattr(obj, attr, bool(v))
    except Exception:
        # どうせフォールバックがあるので握りつぶす
        return


def _sync_track_to_models(db: Session, person: KBPerson, st, v: bool) -> None:
    """
    保存先が Person/State のどちらでも「表示が必ず合う」ように同期する。
    """
    # State側
    if st is not None:
        a = _pick_attr(st, ["track_diary", "diary_track", "is_tracking", "tracking", "track"])
        if a:
            _write_track_value(st, a, v)
        try:
            db.add(st)
        except Exception:
            pass

    # Person側
    a = _pick_attr(person, ["track_diary", "diary_track", "is_tracking", "tracking"])
    if a:
        _write_track_value(person, a, v)
    # 表示用にも確実に instance attr を置く（テンプレでperson.track_diaryを見てしまってもズレにくくする）
    try:
        setattr(person, "track_diary", bool(v))
    except Exception:
        pass

    try:
        db.add(person)
    except Exception:
        pass


def _build_diary_track_map(db: Session, persons: List[KBPerson]) -> dict[int, bool]:
    """
    一覧ページ用：person_id -> tracked(bool)
    - KBDiaryState が有効なら state をまとめて取得して参照
    - 無効なら Person 側のみで判定
    """
    out: dict[int, bool] = {}
    if not persons:
        return out

    person_ids = []
    for p in persons:
        try:
            pid = int(getattr(p, "id", 0) or 0)
        except Exception:
            pid = 0
        if pid > 0:
            person_ids.append(pid)

    state_map = {}
    if diary_state_enabled() and person_ids:
        try:
            state_map = get_diary_state_map(db, person_ids)
        except Exception:
            state_map = {}

    for p in persons:
        try:
            pid = int(getattr(p, "id", 0) or 0)
        except Exception:
            pid = 0
        if pid <= 0:
            continue
        st = state_map.get(pid) if isinstance(state_map, dict) else None
        out[pid] = bool(_read_track(p, st))
    return out


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
        request=request,
        name="kb_index.html",
        context={
            "request": request,
            "kb_allow_secret": _get_kb_allow_secret(),
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
            "diary_track_map": {},  # ★検索結果なしなので空
            "diary_fetch_url_map": {},  # ★DTO www統一（slot埋め込み用）
            "diary_open_url_map": {},   # ★DTO s統一（クリック用）
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

@router.post("/kb/store/{store_id}/update")
def kb_update_store(
    request: Request,
    store_id: int,
    name: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/store/{store_id}"

    s = db.query(KBStore).filter(KBStore.id == int(store_id)).first()
    if not s:
        return RedirectResponse(url="/kb", status_code=303)

    new_name = (name or "").strip()
    if not new_name:
        return RedirectResponse(url=back_url, status_code=303)

    # 同一regionで重複名を禁止（UniqueConstraint対策）
    dup = (
        db.query(KBStore)
        .filter(KBStore.region_id == s.region_id, KBStore.name == new_name, KBStore.id != s.id)
        .first()
    )
    if dup:
        return RedirectResponse(url=back_url + "?store_update=dup", status_code=303)

    try:
        s.name = new_name
        if hasattr(s, "name_norm"):
            s.name_norm = norm_text(new_name)
        db.commit()
    except Exception:
        db.rollback()
        return RedirectResponse(url=back_url + "?store_update=failed", status_code=303)

    return RedirectResponse(url=back_url + "?store_update=done", status_code=303)

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

    # ============================================================
    #  日記最新（epoch ms）をテンプレに渡す
    #   - KBDiaryState の latest_entry_at を使う
    #   - diary_state_enabled() が false なら空
    # ============================================================
    diary_latest_ts_map: dict[int, int] = {}

    if diary_state_enabled() and person_ids:
        try:
            state_map = get_diary_state_map(db, person_ids) or {}
            for pid in person_ids:
                st = state_map.get(int(pid))
                if st is None:
                    continue
                dt = getattr(st, "latest_entry_at", None)
                ms = _dt_to_epoch_ms(dt)
                if ms is not None:
                    diary_latest_ts_map[int(pid)] = ms
        except Exception:
            diary_latest_ts_map = {}

    sort_eff, order_eff = normalize_sort_params(sort, order)

    rmin = parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

    # ★ここが本命：一覧ページ用の追跡map（person_id -> bool）
    diary_track_map = _build_diary_track_map(db, persons)

    # ★DTOのURLを用途別に付与（fetchはwww統一 / openはs統一）
    diary_fetch_url_map, diary_open_url_map = _attach_diary_urls_for_templates(persons)

    return templates.TemplateResponse(
        request=request,
        name="kb_store.html",
        context={
            "request": request,
            "kb_allow_secret": _get_kb_allow_secret(),
            "region": region,
            "store": store,
            "persons": persons,
            "rating_avg_map": rating_avg_map,
            "amount_avg_map": amount_avg_map,
            "last_visit_map": last_visit_map,
            "diary_track_map": diary_track_map,
            "diary_fetch_url_map": diary_fetch_url_map,
            "diary_open_url_map": diary_open_url_map,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
            "sort_options": SORT_OPTIONS,
            "sort": sort_eff,
            "order": order_eff,
            "rating_min": str(rmin) if rmin is not None else "",
            "star_only": "1" if (star_only or "") == "1" else "",
            "diary_latest_ts_map": diary_latest_ts_map,
            "work_start_label": _work_start_label,
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

        # 人物ページの初期値
        p.age = 20
        p.height_cm = 150
        p.cup = "C"
        p.bust_cm = 88
        p.waist_cm = 55
        p.hip_cm = 85
        if hasattr(p, "work_start"):
            p.work_start = "10:00"
            
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

    # ✅ 追跡フラグ：Stateが有効なら必ず作成して読み取る（表示は bool をテンプレへ渡す）
    st = None
    if diary_state_enabled():
        state_map = get_diary_state_map(db, [int(person_id)])
        st = state_map.get(int(person_id))
        st = get_or_create_diary_state(db, state_map, int(person_id)) or st

    track_diary = _read_track(person, st)

    # ★DTO URL: fetch=www / open=s を人物詳細でも渡す
    one_list = [person]
    diary_fetch_url_map, diary_open_url_map = _attach_diary_urls_for_templates(one_list)

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

    work_start_options = [("early", "早朝(10時前)")]
    hh = 10
    mm = 0
    while True:
        work_start_options.append((f"{hh:02d}:{mm:02d}", f"{hh:02d}:{mm:02d}"))
        if hh == 24 and mm == 30:
            break
        mm += 30
        if mm >= 60:
            hh += 1
            mm = 0
    work_start_options.append(("late", "深夜(25時以降)"))

    return templates.TemplateResponse(
        request=request,
        name="kb_person.html",
        context={
            "request": request,
            "kb_allow_secret": _get_kb_allow_secret(),
            "region": region,
            "store": store,
            "person": person,
            "visits": visits,
            "rating_avg": rating_avg,
            "amount_avg_yen": amount_avg_yen,
            "track_diary": track_diary,  # ✅ テンプレはこれを見る（"0" 事故を完全に回避）
            "diary_fetch_url_map": diary_fetch_url_map,
            "diary_open_url_map": diary_open_url_map,
            "google_all_url": google_all_url,
            "google_cityheaven_url": google_cityheaven_url,
            "google_dto_url": google_dto_url,
            "google_all_diary_url": google_all_diary_url,
            "google_cityheaven_diary_url": google_cityheaven_diary_url,
            "google_dto_diary_url": google_dto_diary_url,
            "active_page": "kb",
            "page_title_suffix": "KB",
            "body_class": "page-kb",
            "work_start_options": work_start_options,
            "work_start_label": _work_start_label(getattr(person, "work_start", None)),
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
    work_start: str = Form(""),
    services: str = Form(""),
    tags: str = Form(""),
    url: str = Form(""),
    image_urls_text: str = Form(""),
    memo: str = Form(""),
    next_action: str = Form(""),
    reason_good: str = Form(""),
    reason_bad: str = Form(""),
    reason_next: str = Form(""),
    candidate_rank: str = Form(""),   # 未設定/1..5
    repeat_intent: str = Form(""),    # 未判定/yes/hold/no
    track_diary: str = Form(""),  # フロント name="track_diary"
    diary_track: str = Form(""),  # 互換
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    st = None
    if diary_state_enabled():
        state_map = get_diary_state_map(db, [int(person_id)])
        st = state_map.get(int(person_id))
        st = get_or_create_diary_state(db, state_map, int(person_id)) or st
        # 念のためセッションへ
        try:
            db.add(st)
        except Exception:
            pass

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
        if hasattr(p, "work_start"):
            p.work_start = _normalize_work_start(work_start)

        p.services = (services or "").strip() or None
        p.tags = (tags or "").strip() or None

        if hasattr(p, "url"):
            u = (url or "").strip()
            p.url = u or None
            if hasattr(p, "url_norm"):
                p.url_norm = _normalize_url_for_dup(p.url or "")

        # ============================================================
        # ★重複検出（URL）
        # - url_norm が同じ人が他にいるなら「dup_url」クエリで警告表示へ
        #   ※テンプレ側で表示は次のステップで実装（まずは検出を確実に）
        # ============================================================
        dup_url_ids = []
        try:
            if getattr(p, "url_norm", None):
                qdup = (
                    db.query(KBPerson)
                    .filter(
                        KBPerson.id != int(person_id),
                        KBPerson.url_norm == p.url_norm,
                    )
                    .all()
                )
                dup_url_ids = [str(int(x.id)) for x in qdup if x and getattr(x, "id", None)]
        except Exception:
            dup_url_ids = []


        if hasattr(p, "image_urls"):
            urls = sanitize_image_urls(image_urls_text or "")
            p.image_urls = urls or None

        p.memo = (memo or "").strip() or None

        # ★次アクション & 判断理由テンプレ
        if hasattr(p, "next_action"):
            p.next_action = (next_action or "").strip() or None
        if hasattr(p, "reason_good"):
            p.reason_good = (reason_good or "").strip() or None
        if hasattr(p, "reason_bad"):
            p.reason_bad = (reason_bad or "").strip() or None
        if hasattr(p, "reason_next"):
            p.reason_next = (reason_next or "").strip() or None


        # ============================================================
        # ★意思決定（確定仕様）
        # - candidate_rank: 1..5 / それ以外・空は None
        # - repeat_intent: yes/hold/no / それ以外・空は None
        # - 矛盾防止：repeat が入ったら candidate は必ず None（訪問後フェーズ優先）
        # ============================================================
        ri = (repeat_intent or "").strip().lower()
        if ri in ("yes", "hold", "no"):
            p.repeat_intent = ri
        else:
            p.repeat_intent = None
        
        if p.repeat_intent is not None:
            # 訪問後フェーズ優先：候補ランクは無効化
            p.candidate_rank = None
        else:
            cr_raw = (candidate_rank or "").strip()
            cr = parse_int(cr_raw)
            if cr is not None and 1 <= int(cr) <= 5:
                p.candidate_rank = int(cr)
            else:
                p.candidate_rank = None

     
        # ✅ チェックボックス：ON時は "1" が来る / OFF時は来ない（空文字）
        raw_track = (track_diary or "").strip() or (diary_track or "").strip()
        new_track = _coerce_bool(raw_track)

        old_track = _read_track(p, st)

        # ✅ まず既存ロジックで保存（保存先の正解をプロジェクト側に委ねる）
        try:
            set_person_diary_track(p, new_track, st)
        except Exception:
            pass

        # ✅ そのうえで Person/State の実体にも同期（"0" 文字列事故・反映漏れを潰す）
        _sync_track_to_models(db, p, st, new_track)

        # ✅ OFF→ON のときは未チェック扱いに戻す
        if (not old_track) and new_track:
            try:
                set_person_diary_checked_at(p, None, st)
            except Exception:
                pass

        p.name_norm = norm_text(p.name or "")
        p.services_norm = norm_text(p.services or "")
        p.tags_norm = norm_text(p.tags or "")
        p.memo_norm = norm_text(p.memo or "")

        p.search_norm = build_person_search_blob(db, p)

        # URL重複が見つかった場合は、保存はするが「警告付きで戻す」ためにフラグを立てる
        dup_url_param = ",".join(dup_url_ids) if dup_url_ids else ""

        db.commit()
    except Exception:
        db.rollback()

    # dup_url がある時はクエリで返す（テンプレで警告表示に使う）
    if "dup_url_param" in locals() and dup_url_param:
        sep = "&" if ("?" in back_url) else "?"
        return RedirectResponse(url=f"{back_url}{sep}dup_url={dup_url_param}", status_code=303)

    return RedirectResponse(url=back_url, status_code=303)

@router.post("/kb/person/{person_id}/quick_update")
def kb_quick_update_person(
    request: Request,
    person_id: int,
    candidate_rank: str = Form(""),
    repeat_intent: str = Form(""),
    next_action: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/kb"

    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        # --- repeat_intent: yes/hold/no 以外は None
        ri = (repeat_intent or "").strip().lower()
        if hasattr(p, "repeat_intent"):
            p.repeat_intent = ri if ri in ("yes", "hold", "no") else None

        # --- candidate_rank: 1..5 / repeat が入ってたら必ず None
        if hasattr(p, "candidate_rank"):
            if getattr(p, "repeat_intent", None) is not None:
                p.candidate_rank = None
            else:
                cr = parse_int((candidate_rank or "").strip())
                p.candidate_rank = int(cr) if (cr is not None and 1 <= int(cr) <= 5) else None

        # --- next_action: （設計に合わせて）自由入力 or enum
        if hasattr(p, "next_action"):
            na = (next_action or "").strip()
            p.next_action = na or None

        # 検索用blobを更新（あなたの設計だとここ重要）
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

    # ============================================================
    # 日記最新（epoch ms）をテンプレに渡す
    # ============================================================
    diary_latest_ts_map: dict[int, int] = {}
    person_ids = [int(p.id) for p in persons]

    if diary_state_enabled() and person_ids:
        try:
            state_map = get_diary_state_map(db, person_ids) or {}
            for pid in person_ids:
                st = state_map.get(int(pid))
                if st is None:
                    continue
                dt = getattr(st, "latest_entry_at", None)
                ms = _dt_to_epoch_ms(dt)
                if ms is not None:
                    diary_latest_ts_map[int(pid)] = ms
        except Exception:
            diary_latest_ts_map = {}

    sort_eff, order_eff = normalize_sort_params(sort, order)

    rmin = parse_rating_min(rating_min or "")
    if rmin is None and (star_only or "") == "1":
        rmin = 1

    persons = filter_persons_by_rating_min(persons, rmin, rating_avg_map)
    persons = sort_persons(persons, sort_eff, order_eff, rating_avg_map, amount_avg_map, last_visit_map)

    # ★検索結果一覧用の追跡map（person_id -> bool）
    diary_track_map = _build_diary_track_map(db, persons)

    # ★DTOのURLを用途別に付与（fetchはwww統一 / openはs統一）
    diary_fetch_url_map, diary_open_url_map = _attach_diary_urls_for_templates(persons)

    return templates.TemplateResponse(
        request=request,
        name="kb_index.html",
        context={
            "request": request,
            "kb_allow_secret": _get_kb_allow_secret(),
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
            "diary_track_map": diary_track_map,
            "diary_fetch_url_map": diary_fetch_url_map,
            "diary_open_url_map": diary_open_url_map,
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
            "diary_latest_ts_map": diary_latest_ts_map,
            "work_start_label": _work_start_label,
        },
    )
