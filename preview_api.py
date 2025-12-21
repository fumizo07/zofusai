# preview_api.py
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from db import get_db
from models import ThreadPost, CachedPost  # ★追加：CachedPost を見る
from services import get_thread_posts_cached  # ★追加：キャッシュ未作成なら作る

preview_api = APIRouter()


def _normalize_thread_url_key(raw: str) -> str:
    """
    突合用のキー正規化（できるだけ「同じスレは同じキー」になるように）。
    - rrid=xx が混ざっても落とす
    - http → https に寄せる
    - クエリ/フラグメント除去
    - 末尾スラッシュ統一
    - thr_res_show / thr_res の揺れを吸収できるよう、基本は thr_res に寄せる
      （ただし DB 側が show で保存されているケースもあるので、検索時は両方を見る）
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
        u = "https://" + u[len("http://") :]

    # 表示系パス揺れを吸収（キーは thr_res に寄せる）
    u = u.replace("/thr_res_show/", "/thr_res/")

    # 末尾スラッシュ統一
    if u and not u.endswith("/"):
        u += "/"

    return u


def _alt_show_url(norm_thr_res_url: str) -> str:
    """
    thr_res に寄せたURLから thr_res_show の別キーも作る
    """
    if not norm_thr_res_url:
        return ""
    return norm_thr_res_url.replace("/thr_res/", "/thr_res_show/")


def _format_posted_at(posted_at: str | None) -> str:
    return (posted_at or "").strip()


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

    alt_thread_url = _alt_show_url(norm_thread_url)

    # =========================================================
    # 1) 保存済みスレ（ThreadPost）を最優先で探す
    # =========================================================
    row = (
        db.query(ThreadPost)
        .filter(ThreadPost.thread_url == norm_thread_url, ThreadPost.post_no == post_no)
        .first()
    )
    if row is None and alt_thread_url and alt_thread_url != norm_thread_url:
        row = (
            db.query(ThreadPost)
            .filter(ThreadPost.thread_url == alt_thread_url, ThreadPost.post_no == post_no)
            .first()
        )

    if row is not None:
        body = row.body or ""
        posted_at = ""
        if getattr(row, "posted_at", None):
            posted_at = _format_posted_at(row.posted_at)
        elif getattr(row, "posted_at_dt", None):
            try:
                posted_at = row.posted_at_dt.isoformat(sep=" ", timespec="seconds")
            except Exception:
                posted_at = str(row.posted_at_dt)

        if len(body) > 4000:
            body = body[:4000] + "\n…（省略）"

        return {
            "ok": True,
            "thread_url": norm_thread_url,
            "post_no": post_no,
            "posted_at": posted_at,
            "body": body,
        }

    # =========================================================
    # 2) 外部検索キャッシュ（CachedPost）を探す
    # =========================================================
    c = (
        db.query(CachedPost)
        .filter(CachedPost.thread_url == norm_thread_url, CachedPost.post_no == post_no)
        .first()
    )
    if c is None and alt_thread_url and alt_thread_url != norm_thread_url:
        c = (
            db.query(CachedPost)
            .filter(CachedPost.thread_url == alt_thread_url, CachedPost.post_no == post_no)
            .first()
        )

    if c is not None:
        body = c.body or ""
        posted_at = _format_posted_at(getattr(c, "posted_at", None))

        if len(body) > 4000:
            body = body[:4000] + "\n…（省略）"

        return {
            "ok": True,
            "thread_url": norm_thread_url,
            "post_no": post_no,
            "posted_at": posted_at,
            "body": body,
        }

    # =========================================================
    # 3) それでも無いなら、キャッシュを作ってからもう一回探す
    #    （外部検索直後は基本ヒットするはずだが、キー揺れやTTL切れ対策）
    # =========================================================
    try:
        # get_thread_posts_cached は SSRF 対策済みの is_valid_bakusai_thread_url を通る
        # ※ここは「ユーザーがクリックしたスレ」なので fetch してOKという設計
        posts = get_thread_posts_cached(db, raw_thread_url)  # raw を渡して揺れも吸収
        hit = None
        for p in posts:
            if getattr(p, "post_no", None) == post_no:
                hit = p
                break

        if hit is not None:
            body = getattr(hit, "body", "") or ""
            posted_at = _format_posted_at(getattr(hit, "posted_at", None))

            if len(body) > 4000:
                body = body[:4000] + "\n…（省略）"

            return {
                "ok": True,
                "thread_url": norm_thread_url,
                "post_no": post_no,
                "posted_at": posted_at,
                "body": body,
            }
    except Exception:
        pass

    return JSONResponse({"error": "not_found"}, status_code=404)
