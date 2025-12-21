# preview_api.py
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost

preview_api = APIRouter()


def _normalize_thread_url_key(raw: str) -> str:
    """
    DBの thread_url と突き合わせるための「キー正規化」。
    - thr_res_show → thr_res に寄せる（あなたのDBが thr_res で保存されているため）
    - rrid=xx が混ざっても落とす
    - http → https に寄せる
    - クエリ/フラグメント除去
    - 末尾スラッシュ統一
    """
    u = (raw or "").strip()
    if not u:
        return ""

    # query / fragment を落とす
    u = u.split("#", 1)[0]
    u = u.split("?", 1)[0]

    # rrid= が末尾に付いてるケースがあっても落とす
    u = re.sub(r"rrid=\d+/?$", "", u)

    # http -> https
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]

    # 表示系パス揺れを吸収（重要）
    u = u.replace("/thr_res_show/", "/thr_res/")

    # 末尾スラッシュ統一
    if u and not u.endswith("/"):
        u += "/"

    return u


@preview_api.get("/api/post_preview")
def api_post_preview(
    thread_url: str = Query("", description="対象スレURL"),
    post_no: int = Query(0, ge=1, description="レス番号"),
    db: Session = Depends(get_db),
):
    raw_thread_url = (thread_url or "").strip()
    if not raw_thread_url or post_no <= 0:
        return JSONResponse({"error": "bad_request"}, status_code=400)

    norm_thread_url = _normalize_thread_url_key(raw_thread_url)
    if not norm_thread_url:
        return JSONResponse({"error": "bad_request"}, status_code=400)

    # まず正規化キーで検索
    row = (
        db.query(ThreadPost)
        .filter(ThreadPost.thread_url == norm_thread_url, ThreadPost.post_no == post_no)
        .first()
    )

    # 念のため：もしDB側が thr_res_show で保存されてるスレが混在してたら救う
    if row is None:
        alt = norm_thread_url.replace("/thr_res/", "/thr_res_show/")
        if alt != norm_thread_url:
            row = (
                db.query(ThreadPost)
                .filter(ThreadPost.thread_url == alt, ThreadPost.post_no == post_no)
                .first()
            )

    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    body = row.body or ""

    posted_at = ""
    if getattr(row, "posted_at", None):
        posted_at = row.posted_at or ""
    elif getattr(row, "posted_at_dt", None):
        try:
            posted_at = row.posted_at_dt.isoformat(sep=" ", timespec="seconds")
        except Exception:
            posted_at = str(row.posted_at_dt)

    if len(body) > 4000:
        body = body[:4000] + "\n…（省略）"

    return {
        "ok": True,
        # 返す thread_url も正規化したものに統一（以後のキーズレ防止）
        "thread_url": norm_thread_url,
        "post_no": post_no,
        "posted_at": posted_at,
        "body": body,
    }
