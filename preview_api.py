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
    # 末尾スラッシュ群を 0 or 1 に寄せる（https:// の // は末尾以外なので影響なし）
    url = (url or "").strip()
    if not url:
        return ""
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

    # 末尾スラッシュの揺れを吸収して DB を探す
    u0 = thread_url.strip()
    u1 = u0.rstrip("/")
    u2 = u1 + "/"
    u_http = re.sub(r"^https://", "http://", u0)
    u_https = re.sub(r"^http://", "https://", u0)

    candidates = list({u0, u1, u2, u_http, u_http.rstrip("/"), u_http.rstrip("/") + "/", u_https, u_https.rstrip("/"), u_https.rstrip("/") + "/"})

    row = (
        db.query(ThreadPost)
        .filter(ThreadPost.post_no == post_no)
        .filter(ThreadPost.thread_url.in_(candidates))
        .first()
    )


    # --- 1) DBにあればそれを返す ---
    if row is not None:
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
            "thread_url": row.thread_url,
            "post_no": post_no,
            "posted_at": posted_at,
            "body": body,
        }

    # --- 2) DBになければ、外部表示用のキャッシュ/取得から探す（保存してなくてもプレビュー可能にする） ---
    # ここで services を読む（循環import回避のためローカルimport）
    try:
        from services import get_thread_posts_cached  # noqa
    except Exception:
        return JSONResponse({"error": "not_found"}, status_code=404)

    # get_thread_posts_cached は thread_url をキーにしているはずなので、末尾/表記ゆれ候補を順に試す
    found = None
    found_thread_url = None
    for tu in candidates:
        tu_norm = _normalize_trailing_slashes(tu) or tu
        try:
            posts = get_thread_posts_cached(db, tu_norm)
        except Exception:
            continue

        for p in posts or []:
            try:
                if getattr(p, "post_no", None) == post_no:
                    found = p
                    found_thread_url = tu_norm
                    break
            except Exception:
                continue
        if found is not None:
            break

    if found is None:
        return JSONResponse({"error": "not_found"}, status_code=404)

    body = getattr(found, "body", "") or ""

    posted_at = ""
    if getattr(found, "posted_at", None):
        posted_at = found.posted_at or ""
    elif getattr(found, "posted_at_dt", None):
        try:
            posted_at = found.posted_at_dt.isoformat(sep=" ", timespec="seconds")
        except Exception:
            posted_at = str(found.posted_at_dt)

    if len(body) > 4000:
        body = body[:4000] + "\n…（省略）"

    return {
        "ok": True,
        "thread_url": found_thread_url or thread_url,
        "post_no": post_no,
        "posted_at": posted_at,
        "body": body,
    }
