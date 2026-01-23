# 001
# routers/kb_templates.py
import json
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import KBPriceTemplate, KBStore

router = APIRouter()

_MAX_TEMPLATES_PER_STORE = 200
_MAX_ITEMS = 40
_MAX_NAME_LEN = 60
_MAX_LABEL_LEN = 50


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s or "")).strip()
    return s


def _parse_int(x: Any) -> Optional[int]:
    s = _norm(x)
    if not s:
        return None
    s = s.replace(",", "").replace("_", "")
    s = s.replace("円", "").replace("￥", "").replace("¥", "")
    m = re.search(r"-?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _sanitize_items(items: Any) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        label = _norm(it.get("label", ""))
        amt = _parse_int(it.get("amount", 0))
        if amt is None or amt < 0:
            amt = 0
        if len(label) > _MAX_LABEL_LEN:
            label = label[:_MAX_LABEL_LEN]
        # 空行はスキップ（ただし金額だけでも残したいなら仕様を変える）
        if not label and amt == 0:
            continue
        out.append({"label": label, "amount": int(amt)})
        if len(out) >= _MAX_ITEMS:
            break
    return out


def _template_to_dict(t: KBPriceTemplate) -> dict:
    return {
        "id": int(getattr(t, "id")),
        "store_id": int(getattr(t, "store_id")),
        "name": getattr(t, "name", None),
        "items": getattr(t, "items", None) or [],
        "updated_at_utc": getattr(t, "updated_at", None).strftime("%Y-%m-%dT%H:%M:%SZ") if getattr(t, "updated_at", None) else None,
    }


@router.get("/kb/api/price_templates")
def kb_api_price_templates(
    store_id: int,
    db: Session = Depends(get_db),
):
    st = db.query(KBStore).filter(KBStore.id == int(store_id)).first()
    if not st:
        return JSONResponse({"ok": True, "items": []})

    rows = (
        db.query(KBPriceTemplate)
        .filter(KBPriceTemplate.store_id == int(store_id))
        .order_by(KBPriceTemplate.name.asc())
        .limit(_MAX_TEMPLATES_PER_STORE)
        .all()
    )
    return JSONResponse({"ok": True, "items": [_template_to_dict(x) for x in rows]})


@router.post("/kb/api/price_templates/save")
async def kb_api_price_templates_save(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    JSON:
      {
        "store_id": 123,
        "name": "基本",
        "items": [{"label":"指名","amount":1000}, ...]
      }
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="payload_not_object")

    sid = data.get("store_id", None)
    name = data.get("name", "")
    items = data.get("items", [])

    try:
        sid = int(sid)
    except Exception:
        raise HTTPException(status_code=400, detail="store_id_required")

    st = db.query(KBStore).filter(KBStore.id == int(sid)).first()
    if not st:
        raise HTTPException(status_code=404, detail="store_not_found")

    name = _norm(name)
    if not name:
        raise HTTPException(status_code=400, detail="name_required")
    if len(name) > _MAX_NAME_LEN:
        name = name[:_MAX_NAME_LEN]

    clean_items = _sanitize_items(items)
    if not clean_items:
        raise HTTPException(status_code=400, detail="items_empty")

    # upsert by (store_id, name)
    existing = (
        db.query(KBPriceTemplate)
        .filter(KBPriceTemplate.store_id == int(sid), KBPriceTemplate.name == name)
        .first()
    )

    try:
        if existing:
            existing.items = clean_items
            existing.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(existing)
            return JSONResponse({"ok": True, "id": int(existing.id)})
        else:
            # ざっくり上限（大量保存の事故防止）
            cnt = (
                db.query(KBPriceTemplate)
                .filter(KBPriceTemplate.store_id == int(sid))
                .count()
            )
            if cnt >= _MAX_TEMPLATES_PER_STORE:
                raise HTTPException(status_code=400, detail="too_many_templates")

            obj = KBPriceTemplate(
                store_id=int(sid),
                name=name,
                items=clean_items,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(obj)
            db.commit()
            db.refresh(obj)
            return JSONResponse({"ok": True, "id": int(obj.id)})
    except HTTPException:
        raise
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="save_failed")


@router.post("/kb/api/price_templates/delete")
async def kb_api_price_templates_delete(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    JSON: { "id": 999 }
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="payload_not_object")

    tid = data.get("id", None)
    try:
        tid = int(tid)
    except Exception:
        raise HTTPException(status_code=400, detail="id_required")

    row = db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).first()
    if not row:
        return JSONResponse({"ok": True})

    try:
        db.query(KBPriceTemplate).filter(KBPriceTemplate.id == int(tid)).delete(synchronize_session=False)
        db.commit()
        return JSONResponse({"ok": True})
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="delete_failed")
