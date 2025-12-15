from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text

from app.db.session import get_db
from app.db.models import ThreadPost
from app.web.templates import templates
from app.services.text_utils import simplify_thread_title, normalize_for_search
from app.services.client_service import find_prev_next_thread_urls
from scraper import fetch_posts_from_thread, ScrapingError, get_thread_title


router = APIRouter(prefix="/admin", tags=["admin"])


def fetch_thread_into_db(db: Session, url: str) -> int:
    url = (url or "").strip()
    if not url:
        return 0

    last_no = (
        db.query(sql_text("max(post_no)"))
        .select_from(sql_text("thread_posts"))
        .where(sql_text("thread_url = :u"))
        .params(u=url)
    ).scalar()

    if last_no is None:
        last_no = 0

    thread_title = get_thread_title(url)
    if thread_title:
        thread_title = simplify_thread_title(thread_title)

        db.query(ThreadPost).filter(
            ThreadPost.thread_url == url,
            ThreadPost.thread_title.is_(None),
        ).update(
            {ThreadPost.thread_title: thread_title},
            synchronize_session=False,
        )

    scraped_posts = fetch_posts_from_thread(url)
    count = 0
    for sp in scraped_posts:
        body = (sp.body or "").strip()
        if not body:
            continue
        if sp.post_no is not None and sp.post_no <= last_no:
            continue

        if getattr(sp, "anchors", None):
            anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
        else:
            anchors_str = None

        if sp.post_no is not None:
            existing = (
                db.query(ThreadPost)
                .filter(
                    ThreadPost.thread_url == url,
                    ThreadPost.post_no == sp.post_no,
                )
                .first()
            )
        else:
            existing = (
                db.query(ThreadPost)
                .filter(
                    ThreadPost.thread_url == url,
                    ThreadPost.body == body,
                )
                .first()
            )

        if existing:
            if not existing.posted_at and getattr(sp, "posted_at", None):
                existing.posted_at = sp.posted_at
            if not existing.anchors and anchors_str:
                existing.anchors = anchors_str
            if thread_title and not existing.thread_title:
                existing.thread_title = thread_title
            continue

        db.add(
            ThreadPost(
                thread_url=url,
                thread_title=thread_title,
                thread_title_norm=normalize_for_search(thread_title) if thread_title else None,
                post_no=sp.post_no,
                posted_at=getattr(sp, "posted_at", None),
                body=body,
                body_norm=normalize_for_search(body),
                anchors=anchors_str,
            )
        )
        count += 1

    db.commit()
    return count


@router.get("/fetch", response_class=HTMLResponse)
def fetch_thread_get(request: Request, url: str = ""):
    return templates.TemplateResponse(
        "fetch.html",
        {"request": request, "url": url or "", "imported": None, "error": ""},
    )


@router.post("/fetch", response_class=HTMLResponse)
def fetch_thread_post(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    imported: Optional[int] = None
    error: str = ""
    url = (url or "").strip()
    if url:
        try:
            imported = fetch_thread_into_db(db, url)
        except ScrapingError as e:
            db.rollback()
            error = str(e)
        except Exception as e:
            db.rollback()
            error = f"想定外のエラーが発生しました: {e}"
    else:
        error = "URLが入力されていません。"

    return templates.TemplateResponse(
        "fetch.html",
        {"request": request, "url": url, "imported": imported, "error": error},
    )


@router.post("/refetch")
def refetch_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/delete_thread")
def delete_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        db.query(ThreadPost).filter(ThreadPost.thread_url == url).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


def _add_flag_to_url(back_url: str, key: str) -> str:
    if not back_url:
        return f"/threads?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


@router.post("/fetch_next")
def fetch_next_thread(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/threads"
    url = (url or "").strip()

    if not url:
        return RedirectResponse(url=_add_flag_to_url(back_url, "next_error"), status_code=303)

    _, next_url = find_prev_next_thread_urls(url, "")
    if not next_url:
        return RedirectResponse(url=_add_flag_to_url(back_url, "no_next"), status_code=303)

    try:
        fetch_thread_into_db(db, next_url)
        redirect_to = _add_flag_to_url(back_url, "next_ok")
    except Exception:
        db.rollback()
        redirect_to = _add_flag_to_url(back_url, "next_error")

    return RedirectResponse(url=redirect_to, status_code=303)


@router.get("/backfill_norm", response_class=PlainTextResponse)
def backfill_norm(
    limit: int = 50000,
    db: Session = Depends(get_db),
):
    """
    既存データの body_norm / thread_title_norm / tags_norm を埋めるメンテ用。
    例: GET /admin/backfill_norm?limit=50000
    """
    if limit <= 0:
        limit = 1000

    try:
        rows = (
            db.query(ThreadPost)
            .filter(
                (ThreadPost.body_norm.is_(None))
                | (ThreadPost.thread_title_norm.is_(None))
                | (ThreadPost.tags_norm.is_(None))
            )
            .order_by(ThreadPost.id.asc())
            .limit(limit)
            .all()
        )

        updated = 0
        for p in rows:
            if p.body_norm is None:
                p.body_norm = normalize_for_search(p.body or "")
            if p.thread_title_norm is None:
                p.thread_title_norm = normalize_for_search(p.thread_title or "")
            if p.tags_norm is None:
                p.tags_norm = normalize_for_search(p.tags or "")
            updated += 1

        db.commit()
        return f"ok updated={updated}\n"

    except Exception as e:
        db.rollback()
        return f"error {e}\n"
