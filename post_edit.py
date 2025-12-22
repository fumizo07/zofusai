# post_edit.py
from __future__ import annotations

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost
from utils import parse_tags_input, tags_list_to_csv, normalize_for_search

post_edit_router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _build_tags_norm_csv(tags_list: list[str]) -> str:
    """
    tags_norm は「境界一致」検索のために ",tag1,tag2," 形式にする
    （main.py の like('%,{t},%') と整合）
    """
    norm_list = [normalize_for_search(t) for t in (tags_list or []) if (t or "").strip()]
    norm_list = [t for t in norm_list if t]  # 空を除去
    if not norm_list:
        return ""
    return "," + ",".join(norm_list) + ","


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
        tags_norm_csv = _build_tags_norm_csv(tags_list)

        memo_val = (memo or "").strip()

        # 表示・集計用（そのまま）
        row.tags = tags_csv or None

        # 検索用（正規化・境界一致用）
        # ※ models.ThreadPost に tags_norm 列が存在することが前提
        row.tags_norm = tags_norm_csv or None

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
