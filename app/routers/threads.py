from typing import Dict

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db.session import get_db
from app.db.models import ThreadPost, ThreadMeta
from app.web.templates import templates
from app.services.text_utils import simplify_thread_title
from app.state import RECENT_SEARCHES

router = APIRouter()

@router.get("/threads", response_class=HTMLResponse)
def list_threads(
    request: Request,
    db: Session = Depends(get_db),
):
    rows = (
        db.query(
            ThreadPost.thread_url,
            func.max(ThreadPost.post_no).label("max_no"),
            func.max(ThreadPost.posted_at).label("last_posted_at"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
        )
        .group_by(ThreadPost.thread_url)
        .order_by(func.max(ThreadPost.posted_at).desc())
        .all()
    )

    urls = [r.thread_url for r in rows]
    meta_map: Dict[str, ThreadMeta] = {}
    if urls:
        metas = db.query(ThreadMeta).filter(ThreadMeta.thread_url.in_(urls)).all()
        meta_map = {m.thread_url: m for m in metas}

    threads = []
    for r in rows:
        label = meta_map.get(r.thread_url).label if r.thread_url in meta_map else None
        threads.append(
            {
                "thread_url": r.thread_url,
                "thread_title": simplify_thread_title(r.thread_title or r.thread_url),
                "max_no": r.max_no,
                "last_posted_at": r.last_posted_at,
                "post_count": r.post_count,
                "label": label or "",
            }
        )

    tag_rows = db.query(ThreadPost.tags).filter(ThreadPost.tags.isnot(None)).all()
    tag_counts: Dict[str, int] = {}
    for (tags_str,) in tag_rows:
        if not tags_str:
            continue
        for tag in tags_str.split(","):
            tag = tag.strip()
            if not tag:
                continue
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    popular_tags = [
        {"name": name, "count": count}
        for name, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    ]

    recent_searches_view = list(RECENT_SEARCHES)[::-1]

    info_message = ""
    try:
        params = request.query_params
        if params.get("next_ok"):
            info_message = "次スレを取り込みました。"
        elif params.get("no_next"):
            info_message = "次スレが見つかりませんでした。"
        elif params.get("next_error"):
            info_message = "次スレ取得中にエラーが発生しました。"
    except Exception:
        info_message = ""

    return templates.TemplateResponse(
        "threads.html",
        {
            "request": request,
            "threads": threads,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            "info_message": info_message,
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
