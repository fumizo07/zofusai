
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app_context import templates
from db import get_db
from models import ThreadPost, ThreadMeta, CachedThread, CachedPost
from services import fetch_thread_into_db, find_prev_next_thread_urls, is_valid_bakusai_thread_url
from scraper import ScrapingError


router = APIRouter()


def _add_flag_to_url(back_url: str, key: str) -> str:
    if not back_url:
        return f"/?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


@router.get("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_get(request: Request, url: str = ""):
    return templates.TemplateResponse(
        "fetch.html",
        {"request": request, "url": url or "", "imported": None, "error": ""},
    )


@router.post("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_post(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    imported: Optional[int] = None
    error: str = ""
    url = (url or "").strip()

    if url:
        if not is_valid_bakusai_thread_url(url):
            error = "爆サイのスレURLのみ取り込みできます。"
        else:
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


@router.post("/admin/refetch")
def refetch_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    if not is_valid_bakusai_thread_url(url):
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/admin/delete_thread")
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
        db.query(ThreadMeta).filter(ThreadMeta.thread_url == url).delete(synchronize_session=False)
        db.query(CachedPost).filter(CachedPost.thread_url == url).delete(synchronize_session=False)
        db.query(CachedThread).filter(CachedThread.thread_url == url).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/admin/fetch_next")
def fetch_next_thread(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/threads"
    url = (url or "").strip()

    if not url or not is_valid_bakusai_thread_url(url):
        redirect_to = _add_flag_to_url(back_url, "next_error")
        return RedirectResponse(url=redirect_to, status_code=303)

    _, next_url = find_prev_next_thread_urls(url)
    if not next_url:
        redirect_to = _add_flag_to_url(back_url, "no_next")
        return RedirectResponse(url=redirect_to, status_code=303)

    try:
        fetch_thread_into_db(db, next_url)
        redirect_to = _add_flag_to_url(back_url, "next_ok")
    except Exception:
        db.rollback()
        redirect_to = _add_flag_to_url(back_url, "next_error")

    return RedirectResponse(url=redirect_to, status_code=303)
