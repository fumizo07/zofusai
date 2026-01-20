# 001
# routers/internal_search.py
from __future__ import annotations

from collections import defaultdict
from typing import List, Optional, Dict
from urllib.parse import urlencode

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app_context import templates, RECENT_SEARCHES
from db import get_db
from models import ThreadPost, ThreadMeta
from utils import (
    normalize_for_search,
    highlight_text,
    simplify_thread_title,
    build_store_search_title,
    parse_anchors_csv,
    highlight_with_links,
    build_google_site_search_url,
    parse_tags_input,
)


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


def build_reply_tree(all_posts: List["ThreadPost"], root: "ThreadPost") -> List[dict]:
    replies: Dict[int, List[ThreadPost]] = defaultdict(list)
    for p in all_posts:
        for a in parse_anchors_csv(p.anchors):
            replies[a].append(p)

    result: List[dict] = []
    visited_ids: set[int] = set()

    def dfs(post: ThreadPost, depth: int) -> None:
        if post.id in visited_ids:
            return
        visited_ids.add(post.id)
        if post.id != root.id:
            result.append({"post": post, "depth": depth})
        if post.post_no is None:
            return
        for child in replies.get(post.post_no, []):
            dfs(child, depth + 1)

    if root.post_no is not None:
        for child in replies.get(root.post_no, []):
            dfs(child, 0)
    return result


@router.get("/", response_class=HTMLResponse)
def show_search_page(
    request: Request,
    q: str = "",
    thread_filter: str = "",
    tags: str = "",
    tag_mode: str = "or",
    page: int = 1,
    per_page: int = 50,
    db: Session = Depends(get_db),
):
    from sqlalchemy import or_

    keyword_raw = (q or "").strip()
    thread_filter_raw = (thread_filter or "").strip()
    tags_input_raw = (tags or "").strip()
    tag_mode = (tag_mode or "or").lower()
    if tag_mode not in ("or", "and"):
        tag_mode = "or"

    # ===== ページング安全化（入口） =====
    try:
        page = int(page)
    except Exception:
        page = 1
    if page < 1:
        page = 1

    try:
        per_page = int(per_page)
    except Exception:
        per_page = 50

    allowed_per_pages = (10, 20, 50, 100)
    if per_page not in allowed_per_pages:
        per_page = 50

    # tags（トークン化）
    tags_list: List[str] = parse_tags_input(tags_input_raw)

    # 検索用に正規化
    keyword_norm = normalize_for_search(keyword_raw) if keyword_raw else ""
    thread_filter_norm = normalize_for_search(thread_filter_raw) if thread_filter_raw else ""
    tags_norm_list = [normalize_for_search(t) for t in tags_list] if tags_list else []

    thread_results: List[dict] = []
    hit_count = 0
    shown_count = 0
    error_message: str = ""
    popular_tags: List[dict] = []

    info_message = _get_next_thread_message(request)

    try:
        # ------- popular_tags（tags列を集計して表示） -------
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

        # ------- 検索パラメータがある時だけ検索 -------
        if keyword_raw or thread_filter_raw or tags_input_raw:
            params = {
                "q": keyword_raw,
                "thread_filter": thread_filter_raw,
                "tags": tags_input_raw,
                "tag_mode": tag_mode,
            }
            qs = urlencode(params, doseq=False)
            entry = {"params": params, "url": "/?" + qs}
            if not any(e["url"] == entry["url"] for e in RECENT_SEARCHES):
                RECENT_SEARCHES.append(entry)

            hits_q = db.query(ThreadPost)

            # 本文キーワード（body_norm）
            if keyword_norm:
                hits_q = hits_q.filter(ThreadPost.body_norm.like(f"%{keyword_norm}%"))

            # thread_filter（URL or タイトル）
            if thread_filter_raw:
                url_like = f"%{thread_filter_raw}%"
                if thread_filter_norm:
                    title_like = f"%{thread_filter_norm}%"
                    hits_q = hits_q.filter(
                        or_(
                            ThreadPost.thread_url.ilike(url_like),
                            ThreadPost.thread_title_norm.like(title_like),
                        )
                    )
                else:
                    hits_q = hits_q.filter(ThreadPost.thread_url.ilike(url_like))

            # tags（tags_norm を境界一致で検索：",tag,"）
            if tags_norm_list:
                tag_expr = func.coalesce(ThreadPost.tags_norm, "")
                if tag_mode == "and":
                    for t in tags_norm_list:
                        if not t:
                            continue
                        hits_q = hits_q.filter(tag_expr.like(f"%,{t},%"))
                else:
                    conds = [tag_expr.like(f"%,{t},%") for t in tags_norm_list if t]
                    if conds:
                        hits_q = hits_q.filter(or_(*conds))

            hit_count = hits_q.count()

            last_page = max(1, (hit_count + per_page - 1) // per_page)
            if page > last_page:
                page = last_page
            offset = (page - 1) * per_page

            hits_page: List[ThreadPost] = (
                hits_q.order_by(
                    ThreadPost.thread_url.asc(),
                    func.coalesce(ThreadPost.post_no, 10**9).asc(),
                    ThreadPost.id.asc(),
                )
                .limit(per_page)
                .offset(offset)
                .all()
            )
            shown_count = len(hits_page)

            if hits_page:
                thread_urls = sorted({p.thread_url for p in hits_page if p.thread_url})

                thread_posts: List[ThreadPost] = (
                    db.query(ThreadPost)
                    .filter(ThreadPost.thread_url.in_(thread_urls))
                    .order_by(
                        ThreadPost.thread_url.asc(),
                        func.coalesce(ThreadPost.post_no, 10**9).asc(),
                        ThreadPost.id.asc(),
                    )
                    .all()
                )

                posts_by_thread: Dict[str, List[ThreadPost]] = defaultdict(list)
                for p in thread_posts:
                    posts_by_thread[p.thread_url].append(p)

                metas = db.query(ThreadMeta).filter(ThreadMeta.thread_url.in_(thread_urls)).all()
                meta_map: Dict[str, ThreadMeta] = {m.thread_url: m for m in metas}

                thread_map: Dict[str, dict] = {}

                for root in hits_page:
                    thread_url = root.thread_url
                    block = thread_map.get(thread_url)
                    if not block:
                        title = simplify_thread_title(root.thread_title or thread_url)
                        store_title = build_store_search_title(title)

                        label = ""
                        if thread_url in meta_map and meta_map[thread_url].label:
                            label = meta_map[thread_url].label or ""

                        block = {
                            "thread_url": thread_url,
                            "thread_title": title,
                            "store_title": store_title,
                            "store_cityheaven_url": build_google_site_search_url("cityheaven.net", store_title),
                            "store_dto_url": build_google_site_search_url("dto.jp", store_title),
                            "label": label,
                            "entries": [],
                        }
                        thread_map[thread_url] = block
                        thread_results.append(block)

                    all_posts_thread = posts_by_thread.get(thread_url, [])

                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts_thread:
                        start_no = max(1, root.post_no - 5)
                        end_no = root.post_no + 5
                        context_posts = [
                            p
                            for p in all_posts_thread
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    tree_items = build_reply_tree(all_posts_thread, root)

                    anchor_targets: List[ThreadPost] = []
                    if root.anchors:
                        nums = parse_anchors_csv(root.anchors)
                        if nums and all_posts_thread:
                            num_set = set(nums)
                            anchor_targets = [
                                p
                                for p in all_posts_thread
                                if p.post_no is not None and p.post_no in num_set
                            ]

                    block["entries"].append(
                        {
                            "root": root,
                            "context": context_posts,
                            "tree": tree_items,
                            "anchor_targets": anchor_targets,
                        }
                    )

    except Exception as e:
        db.rollback()
        error_message = f"検索中にエラーが発生しました: {e}"
        thread_results = []
        hit_count = 0
        shown_count = 0

    recent_searches_view = list(RECENT_SEARCHES)[::-1]

    last_page = max(1, (hit_count + per_page - 1) // per_page)
    if page > last_page:
        page = last_page

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword_raw,
            "thread_filter": thread_filter_raw,
            "tags_input": tags_input_raw,
            "tag_mode": tag_mode,
            "results": thread_results,
            "hit_count": hit_count,
            "shown_count": shown_count,
            "page": page,
            "per_page": per_page,
            "last_page": last_page,
            "highlight": highlight_text,
            "error_message": error_message,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            "highlight_with_links": highlight_with_links,
            "info_message": info_message,
        },
    )
