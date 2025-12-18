# preview_api.py
from __future__ import annotations

import re
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost

preview_api = APIRouter()


def _normalize_trailing_slashes(url: str) -> str:
    """
    末尾のスラッシュだけを正規化する。
    - "....//" -> ".../"
    - "...."   -> "...."（末尾スラッシュ無しはそのまま）
    """
    return re.sub(r"/+$", "/", url) if url.endswith("/") else url


@preview_api.get("/api/post_preview")
def api_post_preview(
    thread_url: str = Query("", description="対象スレURL"),
    post_no: int = Query(0, ge=1, description="レス番号"),
    db: Session = Depends(get_db),
):
    thread_url = (thread_url or "").strip()
    if not thread_url or post_no <= 0:
        return JSONResponse({"error": "bad_request"}, status_code=400)

    # ★ここが今回の修正ポイント：末尾スラッシュの揺れを吸収して検索する
    # 例:
    #   DB: ".../tid=123/"  ←保存
    #   JS: ".../tid=123//" ←誤って二重になる
    # などで not_found になるのを防ぐ
    u0 = thread_url
    u1 = u0.rstrip("/")
    u2 = u1 + "/"
    u3 = _normalize_trailing_slashes(u0)

    candidates = list({u0, u1, u2, u3})

    row = (
        db.query(ThreadPost)
        .filter(ThreadPost.post_no == post_no)
        .filter(ThreadPost.thread_url.in_(candidates))
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
        "thread_url": row.thread_url,  # 実際にヒットした保存値を返す（デバッグにも有利）
        "post_no": post_no,
        "posted_at": posted_at,
        "body": body,
    }
