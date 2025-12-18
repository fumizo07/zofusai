# preview_api.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost

preview_api = APIRouter()

@preview_api.get("/api/post_preview")
def api_post_preview(
    thread_url: str = Query("", description="対象スレURL"),
    post_no: int = Query(0, ge=1, description="レス番号"),
    db: Session = Depends(get_db),
):
    thread_url = (thread_url or "").strip()
    if not thread_url or post_no <= 0:
        return JSONResponse({"error": "bad_request"}, status_code=400)

    row = (
        db.query(ThreadPost)
        .filter(ThreadPost.thread_url == thread_url, ThreadPost.post_no == post_no)
        .first()
    )
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    body = row.body or ""

    # posted_at は「文字列 posted_at」があれば優先。なければ posted_at_dt をISOで返す。
    posted_at = ""
    if getattr(row, "posted_at", None):
        posted_at = row.posted_at or ""
    elif getattr(row, "posted_at_dt", None):
        try:
            posted_at = row.posted_at_dt.isoformat(sep=" ", timespec="seconds")
        except Exception:
            posted_at = str(row.posted_at_dt)

    # ツールチップ用途：長すぎると重いので軽く切る
    if len(body) > 4000:
        body = body[:4000] + "\n…（省略）"

    return {
        "ok": True,
        "thread_url": thread_url,
        "post_no": post_no,
        "posted_at": posted_at,
        "body": body,
    }
