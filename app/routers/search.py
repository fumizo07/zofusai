import json
from typing import Dict, List, Optional
from collections import defaultdict
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db.session import get_db
from app.db.models import ThreadPost
from app.web.templates import templates
from app.state import RECENT_SEARCHES
from app.services.text_utils import (
    normalize_for_search,
    simplify_thread_title,
    build_store_search_title,
    build_reply_tree,
    parse_anchors_csv,
    highlight_text,
    highlight_with_links,
)

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
def show_search_page(
    request: Request,
    q: str = "",
    thread_filter: str = "",
    tags: str = "",
    tag_mode: str = "or",
    db: Session = Depends(get_db),
):
    keyword_raw = (q or "").strip()
    thread_filter_raw = (thread_filter or "").strip()
    tags_input_raw = (tags or "").strip()
    tag_mode = (tag_mode or "or").lower()

    keyword_norm = normalize_for_search(keyword_raw)
    thread_filter_norm = normalize_for_search(thread_filter_raw)

    tags_norm_list: List[str] = []
    if tags_input_raw:
        tags_norm_list = [
            normalize_for_search(t)
            for t in tags_input_raw.split(",")
            if t.strip()
        ]

    thread_results: List[dict] = []
    hit_count = 0
    error_message: str = ""
    popular_tags: List[dict] = []
    recent_searches_view: List[dict] = []

    try:
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

            all_posts: List[ThreadPost] = (
                db.query(ThreadPost)
                .order_by(ThreadPost.thread_url.asc(), ThreadPost.post_no.asc())
                .all()
            )

            posts_by_thread: Dict[str, List[ThreadPost]] = defaultdict(list)
            for p in all_posts:
                posts_by_thread[p.thread_url].append(p)

            hits: List[ThreadPost] = []
            for p in all_posts:
                body_norm = normalize_for_search(p.body or "")
                if keyword_norm and keyword_norm not in body_norm:
                    continue

                if thread_filter_norm:
                    url_norm = normalize_for_search(p.thread_url or "")
                    title_norm = normalize_for_search(p.thread_title or "")
                    if thread_filter_norm not in url_norm and thread_filter_norm not in title_norm:
                        continue

                if tags_norm_list:
                    post_tags_norm = normalize_for_search(p.tags or "")
                    if tag_mode == "and":
                        ok = all(t in post_tags_norm for t in tags_norm_list)
                    else:
                        ok = any(t in post_tags_norm for t in tags_norm_list)
                    if not ok:
                        continue

                hits.append(p)

            hit_count = len(hits)

            if hits:
                thread_map: Dict[str, dict] = {}
                for root in hits:
                    thread_url = root.thread_url
                    block = thread_map.get(thread_url)
                    if not block:
                        title = root.thread_title or thread_url
                        title = simplify_thread_title(title)
                        block = {
                            "thread_url": thread_url,
                            "thread_title": title,
                            "store_title": build_store_search_title(title),
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

    recent_searches_view = list(RECENT_SEARCHES)[::-1]

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
            "highlight": highlight_text,
            "error_message": error_message,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            "highlight_with_links": highlight_with_links,
        },
    )


@router.get("/api/search")
def api_search(
    q: str,
    thread_filter: str = "",
    db: Session = Depends(get_db),
):
    keyword = (q or "").strip()
    thread_filter = (thread_filter or "").strip()
    if not keyword:
        return []

    query = db.query(ThreadPost).filter(ThreadPost.body.contains(keyword))
    if thread_filter:
        query = query.filter(ThreadPost.thread_url.contains(thread_filter))

    posts = query.order_by(ThreadPost.id.asc()).all()
    return [
        {
            "id": p.id,
            "thread_url": p.thread_url,
            "thread_title": p.thread_title,
            "post_no": p.post_no,
            "posted_at": p.posted_at,
            "body": p.body,
            "anchors": p.anchors,
            "tags": p.tags,
            "memo": p.memo,
        }
        for p in posts
    ]
