# 001
# routers/kb_parts/price_templates_api.py
from __future__ import annotations

from datetime import datetime
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import or_
from sqlalchemy.orm import Session

from db import get_db
from models import KBPriceTemplate

from .utils import (
    parse_int,
    sanitize_template_name,
    sanitize_price_template_items,
    utc_iso,
    reset_postgres_pk_sequence,
)


router = APIRouter()


@router.get("/kb/api/price_templates")
def kb_api_price_templates(
    store_id: str = "",
    include_global: str = "1",
    sort: str = "name",   # name | updated | created
    order: str = "",      # asc | desc
    db: Session = Depends(get_db),
):
    sid = parse_int(store_id)

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

    base_order = [KBPriceTemplate.store_id.asc().nullsfirst()]

    if sk == "name":
        base_order.append(KBPriceTemplate.name.asc() if od == "asc" else KBPriceTemplate.name.desc())
    elif sk == "created":
        col = getattr(KBPriceTemplate, "created_at", None)
        if col is not None:
            base_order.append(col.asc() if od == "asc" else col.desc())
        else:
            base_order.append(KBPriceTemplate.id.asc() if od == "asc" else KBPriceTemplate.id.desc())
    else:
        col = getattr(KBPriceTemplate, "updated_at", None)
        if col is not None:
            base_order.append(col.asc() if od == "asc" else col.desc())
        else:
            base_order.append(KBPriceTemplate.id.asc() if od == "asc" else KBPriceTemplate.id.desc())

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
                "created_at": utc_iso(getattr(t, "created_at", None)),
                "updated_at": utc_iso(getattr(t, "updated_at", None)),
            }
        )

    return JSONResponse({"ok": True, "items": items})


@router.post("/kb/api/price_templates/save")
async def kb_api_price_templates_save(
    request: Request,
    db: Session = Depends(get_db),
):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    store_id = data.get("store_id", None)
    if store_id in ("", "null"):
        store_id = None
    sid = parse_int(store_id) if store_id is not None else None

    name = sanitize_template_name(data.get("name", ""))
    if not name:
        return JSONResponse({"ok": False, "error": "name_required"}, status_code=400)

    items = sanitize_price_template_items(data.get("items", []))
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
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"ok": False, "error": "payload_not_object"}, status_code=400)

    tid = parse_int(data.get("id", ""))
    new_name = sanitize_template_name(data.get("name", ""))

    if tid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)
    if not new_name:
        return JSONResponse({"ok": False, "error": "name_required"}, status_code=400)

    obj = db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).first()
    if not obj:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    if (getattr(obj, "name", "") or "") == new_name:
        return JSONResponse({"ok": True, "item": {"id": int(obj.id), "store_id": obj.store_id, "name": obj.name, "items": obj.items}})

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
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    tid = None
    if isinstance(data, dict):
        tid = parse_int(data.get("id", ""))
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
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    tid = None
    if isinstance(data, dict):
        tid = parse_int(data.get("id", ""))
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
    sid = parse_int(store_id)

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

    scope_store_ids: set[Optional[int]] = set()
    for it in tpls:
        if not isinstance(it, dict):
            continue
        sid = it.get("store_id", None)
        if sid in ("", "null"):
            sid = None
        sid_i = parse_int(sid) if sid is not None else None
        scope_store_ids.add(int(sid_i) if sid_i is not None else None)

    try:
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

            tid = parse_int(it.get("id", ""))
            sid = it.get("store_id", None)
            if sid in ("", "null"):
                sid = None
            sid_i = parse_int(sid) if sid is not None else None

            name = sanitize_template_name(it.get("name", ""))
            if not name:
                continue

            items = it.get("items", None)
            items_norm = sanitize_price_template_items(items) if items is not None else []
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
        reset_postgres_pk_sequence(db, KBPriceTemplate)
        db.commit()
    except Exception:
        db.rollback()
        return JSONResponse({"ok": False, "error": "import_failed"}, status_code=500)

    return JSONResponse({"ok": True, "inserted": int(inserted)})
