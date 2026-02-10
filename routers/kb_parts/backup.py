# 002
# routers/kb_parts/backup.py
from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from db import get_db
from models import KBPerson, KBRegion, KBStore, KBVisit, KBPriceTemplate

from .diary_core import (
    diary_state_enabled,
    get_diary_state_map,
    get_or_create_diary_state,
    get_person_diary_track,
    get_person_diary_latest_ts,
    get_person_diary_seen_ts,
    get_person_diary_checked_at,
    set_person_diary_checked_at,
    set_person_diary_latest_ts,
    set_person_diary_seen_ts,
)
from .utils import (
    norm_text,
    parse_amount_int,
    parse_int,
    parse_minutes_or_hhmm,
    reset_postgres_pk_sequence,
    sanitize_price_template_items,
    sanitize_template_name,
    build_person_search_blob,
    build_visit_search_blob,
    calc_duration,
)


router = APIRouter()


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
        if diary_state_enabled():
            try:
                from models import KBDiaryState  # type: ignore

                db.query(KBDiaryState).delete(synchronize_session=False)  # type: ignore
            except Exception:
                pass
        db.commit()
    except Exception:
        db.rollback()
        return RedirectResponse(url="/kb?panic=failed", status_code=303)

    return RedirectResponse(url="/kb?panic=done", status_code=303)


@router.get("/kb/export")
def kb_export(db: Session = Depends(get_db)):
    regions = db.query(KBRegion).order_by(KBRegion.id.asc()).all()
    stores = db.query(KBStore).order_by(KBStore.id.asc()).all()
    persons = db.query(KBPerson).order_by(KBPerson.id.asc()).all()
    visits = db.query(KBVisit).order_by(KBVisit.id.asc()).all()
    price_templates = db.query(KBPriceTemplate).order_by(KBPriceTemplate.id.asc()).all()

    person_ids = [int(getattr(p, "id")) for p in persons if p and getattr(p, "id", None)]
    state_map = get_diary_state_map(db, person_ids)

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
            # ★意思決定（候補ランク / リピ意思）
            # - カラムが無い構成でも落ちないよう hasattr でガード
            if hasattr(p, "candidate_rank"):
                d["candidate_rank"] = getattr(p, "candidate_rank", None)
            if hasattr(p, "repeat_intent"):
                d["repeat_intent"] = getattr(p, "repeat_intent", None)
        }

        # ★ favorite（お気に入り）
        if hasattr(p, "favorite"):
            try:
                d["favorite"] = bool(getattr(p, "favorite", False))
            except Exception:
                d["favorite"] = False

        # URL / image_urls
        if hasattr(p, "url"):
            d["url"] = getattr(p, "url", None)
        if hasattr(p, "image_urls"):
            d["image_urls"] = getattr(p, "image_urls", None)

        # ★ diary fields（diary_core の getter で統一：DB state / person列 どちらでもOK）
        track = get_person_diary_track(p, st)
        latest_ts = get_person_diary_latest_ts(p, st)
        seen_ts = get_person_diary_seen_ts(p, st)
        checked_at = get_person_diary_checked_at(p, st)

        d["diary_track"] = bool(track)
        d["diary_latest_ts_ms"] = int(latest_ts) if latest_ts is not None else None
        d["diary_seen_ts_ms"] = int(seen_ts) if seen_ts is not None else None
        try:
            d["diary_checked_at_utc"] = checked_at.strftime("%Y-%m-%dT%H:%M:%SZ") if checked_at else None
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
        "version": 5,  # ★ candidate_rank / repeat_intent を追加
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
    mode: str = Form("replace"),
    db: Session = Depends(get_db),
):
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
        db.query(KBPriceTemplate).delete(synchronize_session=False)
        db.query(KBVisit).delete(synchronize_session=False)
        db.query(KBPerson).delete(synchronize_session=False)
        db.query(KBStore).delete(synchronize_session=False)
        db.query(KBRegion).delete(synchronize_session=False)
        if diary_state_enabled():
            try:
                from models import KBDiaryState  # type: ignore

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
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    diary_payloads: list[dict] = []

    try:
        for r in regions if isinstance(regions, list) else []:
            if not isinstance(r, dict):
                continue
            rid = parse_int(r.get("id", ""))
            name = (r.get("name", "") or "").strip()
            if rid is None or not name:
                continue
            obj = KBRegion(id=int(rid), name=name, name_norm=norm_text(name))
            db.add(obj)
        db.flush()

        for s in stores if isinstance(stores, list) else []:
            if not isinstance(s, dict):
                continue
            sid = parse_int(s.get("id", ""))
            rid = parse_int(s.get("region_id", ""))
            name = (s.get("name", "") or "").strip()
            if sid is None or rid is None or not name:
                continue
            obj = KBStore(id=int(sid), region_id=int(rid), name=name, name_norm=norm_text(name))
            for k in ["area", "board_category", "board_id"]:
                if hasattr(obj, k) and k in s:
                    setattr(obj, k, s.get(k))
            db.add(obj)
        db.flush()

        for t in price_templates if isinstance(price_templates, list) else []:
            if not isinstance(t, dict):
                continue
            tid = parse_int(t.get("id", ""))
            sid = t.get("store_id", None)
            if sid in ("", "null"):
                sid = None
            sid_i = parse_int(sid) if sid is not None else None

            name = sanitize_template_name(t.get("name", ""))
            if tid is None or not name:
                continue

            items = t.get("items", None)
            if items is None:
                items_payload = None
            else:
                items_norm = sanitize_price_template_items(items)
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

        person_objs: List[KBPerson] = []
        for p in persons if isinstance(persons, list) else []:
            if not isinstance(p, dict):
                continue
            pid = parse_int(p.get("id", ""))
            sid = parse_int(p.get("store_id", ""))
            name = (p.get("name", "") or "").strip()
            if pid is None or sid is None or not name:
                continue

            obj = KBPerson(id=int(pid), store_id=int(sid), name=name)
            obj.age = parse_int(p.get("age", ""))
            obj.height_cm = parse_int(p.get("height_cm", ""))
            cu = unicodedata.normalize("NFKC", str(p.get("cup", "") or "")).upper().strip()
            obj.cup = (cu[:1] if cu and "A" <= cu[:1] <= "Z" else None)
            obj.bust_cm = parse_int(p.get("bust_cm", ""))
            obj.waist_cm = parse_int(p.get("waist_cm", ""))
            obj.hip_cm = parse_int(p.get("hip_cm", ""))
            obj.services = (p.get("services", "") or "").strip() or None
            obj.tags = (p.get("tags", "") or "").strip() or None
            obj.memo = (p.get("memo", "") or "").strip() or None
            # ★意思決定（候補ランク / リピ意思）
            # 仕様：
            # - repeat_intent: yes/hold/no 以外は None
            # - repeat_intent が入ったら candidate_rank は必ず None（訪問後フェーズ優先）
            ri = str(p.get("repeat_intent", "") or "").strip().lower()
            if hasattr(obj, "repeat_intent"):
                obj.repeat_intent = ri if ri in ("yes", "hold", "no") else None

            if hasattr(obj, "candidate_rank"):
                if getattr(obj, "repeat_intent", None) is not None:
                    obj.candidate_rank = None
                else:
                    cr = parse_int(p.get("candidate_rank", ""))
                    obj.candidate_rank = int(cr) if (cr is not None and 1 <= int(cr) <= 5) else None


            # ★ favorite（お気に入り）
            if hasattr(obj, "favorite"):
                try:
                    obj.favorite = bool(p.get("favorite") or False)
                except Exception:
                    obj.favorite = False

            if hasattr(obj, "url"):
                u = (p.get("url", "") or "").strip()
                obj.url = u or None
                if hasattr(obj, "url_norm"):
                    obj.url_norm = norm_text(obj.url or "")

            if hasattr(obj, "image_urls"):
                iu = p.get("image_urls", None)
                if isinstance(iu, list):
                    obj.image_urls = [str(x or "").strip() for x in iu if str(x or "").strip()] or None

            # ★ diary（旧JSON/新JSONどっちでも拾う）
            diary_payloads.append(
                {
                    "person_id": int(pid),
                    "track": bool(p.get("diary_track") or False),
                    "latest_ts_ms": parse_int(p.get("diary_latest_ts_ms", "")),
                    "seen_ts_ms": parse_int(p.get("diary_seen_ts_ms", "")),
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

        # ★ diary state の復元（diary_core の setter で統一）
        if diary_payloads:
            # まず state_map を取得（必要なら作る）
            ids_for_state = [int(it.get("person_id")) for it in diary_payloads if it.get("person_id")]
            ids_for_state = list(dict.fromkeys([i for i in ids_for_state if i > 0]))
            state_map = get_diary_state_map(db, ids_for_state) if ids_for_state else {}

            # person_id -> person obj
            pmap = {int(getattr(p, "id", 0)): p for p in person_objs if p and getattr(p, "id", None)}

            for it in diary_payloads:
                pid = int(it.get("person_id") or 0)
                if pid <= 0:
                    continue
                p = pmap.get(pid)
                if not p:
                    continue

                st = state_map.get(pid)
                if diary_state_enabled():
                    st = get_or_create_diary_state(db, state_map, pid) or st

                # track
                try:
                    # note: diary_core が「state優先/互換person列」どちらも扱う前提
                    if hasattr(p, "diary_track") or (st is not None):
                        # 追跡ON/OFFは setter が無い設計もあり得るので、trackは互換的に反映
                        # ここは「KBPerson側の古い列」が無い可能性もあるため、可能なら state に直接書く
                        # → diary_core の get_person_diary_track が st を見てくれる設計なら十分
                        if st is not None:
                            for k in ("track_enabled", "track", "diary_track"):
                                if hasattr(st, k):
                                    setattr(st, k, bool(it.get("track") or False))
                                    break
                        elif hasattr(p, "diary_track"):
                            setattr(p, "diary_track", bool(it.get("track") or False))
                except Exception:
                    pass

                # checked_at
                cd = it.get("checked_at", None)
                if isinstance(cd, datetime):
                    set_person_diary_checked_at(p, cd, st)

                # latest/seen
                latest_ts = it.get("latest_ts_ms", None)
                seen_ts = it.get("seen_ts_ms", None)

                if latest_ts is not None:
                    try:
                        set_person_diary_latest_ts(p, int(latest_ts), st)
                    except Exception:
                        pass

                # seen が無い場合は latest で初期化（初回からNEWにしない）
                if seen_ts is None and latest_ts is not None:
                    seen_ts = latest_ts

                if seen_ts is not None:
                    try:
                        set_person_diary_seen_ts(p, int(seen_ts), st)
                    except Exception:
                        pass

        db.flush()

        for obj in person_objs:
            try:
                obj.search_norm = build_person_search_blob(db, obj)
            except Exception:
                obj.search_norm = norm_text(obj.name or "")

        for v in visits if isinstance(visits, list) else []:
            if not isinstance(v, dict):
                continue
            vid = parse_int(v.get("id", ""))
            pid = parse_int(v.get("person_id", ""))
            if vid is None or pid is None:
                continue

            dt = None
            vd = (v.get("visited_at", "") or "").strip()
            if vd:
                try:
                    dt = datetime.strptime(vd, "%Y-%m-%d")
                except Exception:
                    dt = None

            stt = parse_minutes_or_hhmm(v.get("start_time", None))
            enn = parse_minutes_or_hhmm(v.get("end_time", None))
            dur = parse_minutes_or_hhmm(v.get("duration_min", None))
            if dur is None:
                dur = calc_duration(stt, enn)

            rt = parse_int(v.get("rating", ""))
            if rt is not None and not (1 <= int(rt) <= 5):
                rt = None

            price_items_raw = v.get("price_items", None)
            price_items_norm = None
            if isinstance(price_items_raw, list):
                items_tmp = []
                for it in price_items_raw:
                    if not isinstance(it, dict):
                        continue
                    label = str(it.get("label", "") or "").strip()
                    amt_i = parse_amount_int(it.get("amount", 0))
                    if not label and amt_i == 0:
                        continue
                    items_tmp.append({"label": label, "amount": amt_i})
                price_items_norm = items_tmp or None

            total_yen = parse_int(v.get("total_yen", "")) or 0
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

        reset_postgres_pk_sequence(db, KBRegion)
        reset_postgres_pk_sequence(db, KBStore)
        reset_postgres_pk_sequence(db, KBPriceTemplate)
        reset_postgres_pk_sequence(db, KBPerson)
        reset_postgres_pk_sequence(db, KBVisit)
        if diary_state_enabled():
            try:
                from models import KBDiaryState  # type: ignore

                reset_postgres_pk_sequence(db, KBDiaryState)  # type: ignore
            except Exception:
                pass

        db.commit()
    except Exception:
        db.rollback()
        return _redir("failed", "import_failed")

    return _redir("done")
