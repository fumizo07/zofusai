# post_edit.py
from __future__ import annotations

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost
from utils import parse_tags_input, tags_list_to_csv

post_edit_router = APIRouter()
templates = Jinja2Templates(directory="templates")


@post_edit_router.get("/post/{post_id}/edit", response_class=HTMLResponse)
def edit_post_get(
    post_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    row = db.get(ThreadPost, post_id)
    if not row:
        raise HTTPException(status_code=404, detail="not_found")

    return templates.TemplateResponse(
        "post_edit.html",
        {
            "request": request,
            "post": row,
            "saved": False,
            "error": "",
        },
    )


@post_edit_router.post("/post/{post_id}/edit", response_class=HTMLResponse)
def edit_post_post(
    post_id: int,
    request: Request,
    tags: str = Form(""),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    row = db.get(ThreadPost, post_id)
    if not row:
        raise HTTPException(status_code=404, detail="not_found")

    error = ""
    try:
        tags_list = parse_tags_input(tags)
        tags_csv = tags_list_to_csv(tags_list)
        memo_val = (memo or "").strip()

        row.tags = tags_csv or None
        row.memo = memo_val or None

        db.commit()
    except Exception as e:
        db.rollback()
        error = str(e)

    return templates.TemplateResponse(
        "post_edit.html",
        {
            "request": request,
            "post": row,
            "saved": (error == ""),
            "error": error,
        },
    )


@post_edit_router.post("/post/{post_id}/edit/cancel")
def edit_post_cancel(
    post_id: int,
    request: Request,
):
    back_url = request.headers.get("referer") or "/"
    return RedirectResponse(url=back_url, status_code=303)
