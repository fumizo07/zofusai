# 003
# routers/kb_parts/diary_api.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import KBPerson, KBRegion, KBStore

from .diary_core import (
    diary_db_recheck_interval_sec,
    diary_state_enabled,
    parse_ids_csv,
    get_diary_state_map,
    get_or_create_diary_state,
    get_latest_diary_ts_ms,  # diary_core 側（Playwright優先→urllibフォールバック）
    get_person_diary_checked_at,
    get_person_diary_latest_ts,
    get_person_diary_seen_ts,
    get_person_diary_track,
    set_person_diary_checked_at,
    set_person_diary_latest_ts,
    set_person_diary_seen_ts,
    build_diary_open_url_from_maps,
)
from .utils import parse_int


router = APIRouter()


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "on", "yes", "y", "t")


def _server_fetch_disabled() -> bool:
    # 403地獄を止めたい時は Render の env に KB_DIARY_DISABLE_SERVER_FETCH=1
    return _env_bool("KB_DIARY_DISABLE_SERVER_FETCH", default=False)


def _get_push_token() -> str:
    return (os.getenv("KB_DIARY_PUSH_TOKEN", "") or "").strip()


def _require_push_auth(request: Request) -> Optional[str]:
    """
    Returns error string if unauthorized, else None.
    """
    token = _get_push_token()
    if not token:
        return "server_token_not_set"
    got = (request.headers.get("X-KB-Diary-Token") or "").strip()
    if not got or got != token:
        return "unauthorized"
    return None


@router.get("/kb/api/diary_latest")
def kb_api_diary_latest(
    ids: str = Query(""),
    db: Session = Depends(get_db),
):
    person_ids = parse_ids_csv(ids, limit=30)
    if not person_ids:
        return JSONResponse({"ok": True, "items": []})

    persons = db.query(KBPerson).filter(KBPerson.id.in_(person_ids)).all()
    pmap = {int(getattr(p, "id", 0)): p for p in persons if p and getattr(p, "id", None)}

    state_map = get_diary_state_map(db, person_ids)

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

    disable_fetch = _server_fetch_disabled()

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
                    "checked_ago_min": None,
                    "latest_ago_days": None,
                }
            )
            continue

        tracked = get_person_diary_track(p, st)

        st_store = store_map.get(int(getattr(p, "store_id", 0) or 0))
        rg = region_map.get(int(getattr(st_store, "region_id", 0) or 0)) if st_store else None
        open_url = build_diary_open_url_from_maps(p, st_store, rg)

        if not tracked:
            items.append(
                {
                    "id": int(pid),
                    "tracked": False,
                    "latest_ts": None,
                    "seen_ts": get_person_diary_seen_ts(p, st),
                    "is_new": False,
                    "open_url": open_url,
                    "error": "not_tracked",
                    "checked_ago_min": None,
                    "latest_ago_days": None,
                }
            )
            continue

        pu = ""
        if hasattr(p, "url"):
            pu = (getattr(p, "url", "") or "").strip()
        if not pu:
            items.append(
                {
                    "id": int(pid),
                    "tracked": True,
                    "latest_ts": None,
                    "seen_ts": get_person_diary_seen_ts(p, st),
                    "is_new": False,
                    "open_url": open_url,
                    "error": "url_empty",
                    "checked_ago_min": None,
                    "latest_ago_days": None,
                }
            )
            continue

        if diary_state_enabled():
            st = get_or_create_diary_state(db, state_map, int(pid)) or st

        latest_ts = get_person_diary_latest_ts(p, st)
        checked_at = get_person_diary_checked_at(p, st)

        need_fetch = True
        if disable_fetch:
            need_fetch = False
        elif checked_at and latest_ts is not None:
            try:
                age_sec = (now_utc - checked_at).total_seconds()
                if age_sec >= 0 and age_sec < float(diary_db_recheck_interval_sec()):
                    need_fetch = False
            except Exception:
                need_fetch = True

        err = ""
        if need_fetch:
            latest_ts_fetched, err = get_latest_diary_ts_ms(pu)

            if set_person_diary_checked_at(p, now_utc, st):
                dirty = True

            if latest_ts_fetched is not None:
                if set_person_diary_latest_ts(p, latest_ts_fetched, st):
                    dirty = True
                latest_ts = latest_ts_fetched

        seen_ts = get_person_diary_seen_ts(p, st)
        is_new = False

        if latest_ts is not None:
            if seen_ts is None:
                if set_person_diary_seen_ts(p, latest_ts, st):
                    dirty = True
                seen_ts = latest_ts
                is_new = False
            else:
                try:
                    is_new = int(latest_ts) > int(seen_ts)
                except Exception:
                    is_new = False

        checked_ago_min = None
        try:
            if checked_at:
                age_sec2 = (now_utc - checked_at).total_seconds()
                if age_sec2 >= 0:
                    checked_ago_min = int(age_sec2 // 60)
        except Exception:
            checked_ago_min = None

        latest_ago_days = None
        try:
            if latest_ts is not None:
                dt_latest = datetime.utcfromtimestamp(int(latest_ts) / 1000.0)
                dsec = (now_utc - dt_latest).total_seconds()
                if dsec >= 0:
                    latest_ago_days = int(dsec // 86400)
        except Exception:
            latest_ago_days = None

        items.append(
            {
                "id": int(pid),
                "tracked": True,
                "latest_ts": latest_ts,
                "seen_ts": seen_ts,
                "is_new": bool(is_new),
                "open_url": open_url,
                "error": err,
                "checked_ago_min": checked_ago_min,
                "latest_ago_days": latest_ago_days,
                "server_fetch_disabled": bool(disable_fetch),
            }
        )

    if dirty:
        try:
            db.commit()
        except Exception:
            db.rollback()

    return JSONResponse({"ok": True, "items": items})


@router.post("/kb/api/diary_push")
async def kb_api_diary_push(
    request: Request,
    db: Session = Depends(get_db),
):
    auth_err = _require_push_auth(request)
    if auth_err:
        return JSONResponse({"ok": False, "error": auth_err}, status_code=401)

    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    items = None
    if isinstance(data, dict):
        items = data.get("items")
    if not isinstance(items, list):
        return JSONResponse({"ok": False, "error": "items_required"}, status_code=400)

    # 受け取り上限（暴走防止）
    if len(items) > 50:
        items = items[:50]

    # まとめて person を引く
    ids: List[int] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        pid = parse_int(it.get("id", ""))
        if pid is None or pid <= 0:
            continue
        ids.append(int(pid))

    ids = list(dict.fromkeys(ids))
    if not ids:
        return JSONResponse({"ok": True, "saved": 0})

    persons = db.query(KBPerson).filter(KBPerson.id.in_(ids)).all()
    pmap = {int(getattr(p, "id", 0)): p for p in persons if p and getattr(p, "id", None)}
    state_map = get_diary_state_map(db, ids)

    saved = 0
    dirty = False

    for it in items:
        if not isinstance(it, dict):
            continue

        pid = parse_int(it.get("id", ""))
        if pid is None or pid <= 0:
            continue
        pid_i = int(pid)

        p = pmap.get(pid_i)
        if not p:
            continue

        st = state_map.get(pid_i)
        if diary_state_enabled():
            st = get_or_create_diary_state(db, state_map, pid_i) or st

        # tracked 以外は保存しない（仕様）
        if not get_person_diary_track(p, st):
            continue

        latest_ts = parse_int(it.get("latest_ts", ""))
        err = (it.get("error", "") or "").strip()
        checked_at_ms = parse_int(it.get("checked_at_ms", ""))

        # checked_at はクライアントが送ってくればそれ、なければ今
        checked_dt = None
        try:
            if checked_at_ms is not None and checked_at_ms > 0:
                checked_dt = datetime.utcfromtimestamp(int(checked_at_ms) / 1000.0)
            else:
                checked_dt = datetime.utcnow()
        except Exception:
            checked_dt = datetime.utcnow()

        if set_person_diary_checked_at(p, checked_dt, st):
            dirty = True

        # latest_ts が取れた時だけ更新
        if latest_ts is not None and latest_ts > 0:
            if set_person_diary_latest_ts(p, int(latest_ts), st):
                dirty = True

        # seen_ts 初期化（未設定なら latest_ts で初期化して「初回からNEW」にならないようにする）
        if latest_ts is not None and latest_ts > 0:
            seen_ts = get_person_diary_seen_ts(p, st)
            if seen_ts is None:
                if set_person_diary_seen_ts(p, int(latest_ts), st):
                    dirty = True

        saved += 1

    if dirty:
        try:
            db.commit()
        except Exception:
            db.rollback()
            return JSONResponse({"ok": False, "error": "db_error"}, status_code=500)

    return JSONResponse({"ok": True, "saved": int(saved)})


@router.post("/kb/api/diary_seen")
async def kb_api_diary_seen(
    request: Request,
    db: Session = Depends(get_db),
):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    pid = None
    if isinstance(data, dict):
        pid = parse_int(data.get("id", ""))
    if pid is None:
        return JSONResponse({"ok": False, "error": "id_required"}, status_code=400)

    p = db.query(KBPerson).filter(KBPerson.id == int(pid)).first()
    if not p:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)

    state_map = get_diary_state_map(db, [int(pid)])
    st = state_map.get(int(pid))
    if diary_state_enabled():
        st = get_or_create_diary_state(db, state_map, int(pid)) or st

    latest_ts = get_person_diary_latest_ts(p, st)
    if latest_ts is None:
        return JSONResponse({"ok": False, "error": "latest_not_ready"}, status_code=409)

    changed = set_person_diary_seen_ts(p, int(latest_ts), st)
    if not changed:
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
    person_ids = parse_ids_csv(ids, limit=30)
    if not person_ids:
        return JSONResponse({"ok": True, "items": []})

    persons = db.query(KBPerson).filter(KBPerson.id.in_(person_ids)).all()
    pmap = {int(getattr(p, "id", 0)): p for p in persons if p and getattr(p, "id", None)}
    state_map = get_diary_state_map(db, person_ids)

    out = []
    for pid in person_ids:
        p = pmap.get(int(pid))
        st = state_map.get(int(pid))
        if not p:
            out.append({"id": int(pid), "is_new": False})
            continue

        if not get_person_diary_track(p, st):
            out.append({"id": int(pid), "is_new": False})
            continue

        latest_ts = get_person_diary_latest_ts(p, st)
        seen_ts = get_person_diary_seen_ts(p, st)

        is_new = False
        if latest_ts is not None and seen_ts is not None:
            try:
                is_new = int(latest_ts) > int(seen_ts)
            except Exception:
                is_new = False

        out.append({"id": int(pid), "is_new": bool(is_new)})

    return JSONResponse({"ok": True, "items": out})
