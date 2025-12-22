# routers/threads.py
from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app_context import templates, RECENT_SEARCHES
from db import get_db
from models import ThreadPost, ThreadMeta, CachedThread
from utils import simplify_thread_title


router = APIRouter()


def _get_next_thread_message(request: Request) -> str:
    try:
        params = request.query_params
        if params.get("next_ok"):
            return "次スレを取り込みました。"
        if params.get("no_next"):
            return "次スレが見つかりませんでした。"
        if params.get("next_error"):
            return "次スレ取得中にエラーが発生しました。"
    except Exception:
        pass
    return ""


@router.get("/threads", response_class=HTMLResponse)
def list_threads(
    request: Request,
    label: str = "",
    db: Session = Depends(get_db),
):
    label = (label or "").strip()

    agg = (
        db.query(
            ThreadPost.thread_url.label("thread_url"),
            func.max(ThreadPost.post_no).label("max_no"),
            func.max(ThreadPost.posted_at_dt).label("last_posted_at_dt"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
            func.max(ThreadPost.id).label("max_id"),
        )
        .group_by(ThreadPost.thread_url)
        .subquery()
    )

    q = (
        db.query(
            agg.c.thread_url,
            agg.c.max_no,
            agg.c.last_posted_at_dt,
            agg.c.thread_title,
            agg.c.post_count,
            ThreadMeta.label.label("label"),
            CachedThread.fetched_at.label("last_fetched_at"),
        )
        .outerjoin(ThreadMeta, ThreadMeta.thread_url == agg.c.thread_url)
        .outerjoin(CachedThread, CachedThread.thread_url == agg.c.thread_url)
    )

    if label:
        q = q.filter(ThreadMeta.label == label)

    rows = (
        q.order_by(
            agg.c.last_posted_at_dt.desc().nullslast(),
            agg.c.max_id.desc(),
        )
        .all()
    )

    threads = []
    for r in rows:
        threads.append(
            {
                "thread_url": r.thread_url,
                "thread_title": simplify_thread_title(r.thread_title or r.thread_url),
                "max_no": r.max_no,
                "last_posted_at": r.last_posted_at_dt,
                "post_count": r.post_count,
                "label": r.label or "",
                "last_fetched_at": r.last_fetched_at,
            }
        )

    tag_rows = db.query(ThreadPost.tags).filter(ThreadPost.tags.isnot(None)).all()
    tag_counts: Dict[str, int] = {}
    for (tags_str,) in tag_rows:
        if not tags_str:
            continue
        for tag_item in tags_str.split(","):
            tag_item = (tag_item or "").strip()
            if not tag_item:
                continue
            tag_counts[tag_item] = tag_counts.get(tag_item, 0) + 1

    popular_tags = [
        {"name": name, "count": count}
        for name, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    ]

    recent_searches_view = list(RECENT_SEARCHES)[::-1]
    info_message = _get_next_thread_message(request)

    return templates.TemplateResponse(
        "threads.html",
        {
            "request": request,
            "threads": threads,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            "info_message": info_message,
            "label_filter": label,
        },
    )


@router.post("/threads/label")
def update_thread_label(
    request: Request,
    thread_url: str = Form(""),
    label: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/threads"
    url = (thread_url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    label = (label or "").strip()
    try:
        meta = db.query(ThreadMeta).filter(ThreadMeta.thread_url == url).first()
        if not meta:
            meta = ThreadMeta(thread_url=url, label=label or None)
            db.add(meta)
        else:
            meta.label = label or None
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)
