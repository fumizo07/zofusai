import json
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from collections import defaultdict

from app.db.session import get_db
from app.web.templates import templates
from app.state import EXTERNAL_SEARCHES
from app.constants.catalog import (
    AREA_OPTIONS,
    PERIOD_OPTIONS,
    BOARD_CATEGORY_OPTIONS,
    BOARD_MASTER,
    get_period_days,
    get_board_options_for_category,
)
from app.services.client_service import (
    search_threads_external,
    _is_valid_bakusai_thread_url,
    find_prev_next_thread_urls,
)
from app.services.cache_service import get_thread_posts_cached
from app.services.text_utils import (
    simplify_thread_title,
    build_store_search_title,
    normalize_for_search,
    highlight_text,
    highlight_with_links,
)
from scraper import get_thread_title
from ranking import get_board_ranking, RANKING_URL_TEMPLATE

router = APIRouter()

def _add_flag_to_url(back_url: str, key: str) -> str:
    if not back_url:
        return f"/thread_search?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


@router.get("/thread_search", response_class=HTMLResponse)
def thread_search_page(
    request: Request,
    area: str = "7",
    period: str = "3m",
    keyword: str = "",
    board_category: str = "103",
    board_id: str = "5922",
):
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"
    keyword = (keyword or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    results: List[dict] = []
    error_message = ""

    ranking_board = None
    ranking_board_label = ""
    ranking_source_url = ""

    board_options = get_board_options_for_category(board_category)

    if keyword and area:
        max_days = get_period_days(period)
        try:
            results = search_threads_external(
                area_code=area,
                keyword=keyword,
                max_days=max_days,
                board_category=board_category,
                board_id=board_id,
            )
        except Exception as e:
            error_message = f"外部検索中にエラーが発生しました: {e}"

        if not error_message and board_category and board_id:
            board_label = ""
            for b in board_options:
                if b["id"] == board_id:
                    board_label = b["label"]
                    break

            ranking_board_label = board_label or "選択した板"
            ranking_board = get_board_ranking(area, board_category, board_id)

            if ranking_board:
                ranking_source_url = RANKING_URL_TEMPLATE.format(
                    acode=area,
                    ctgid=board_category,
                    bid=board_id,
                )

        if not error_message:
            area_label = next((a["label"] for a in AREA_OPTIONS if a["code"] == area), area)
            period_label = next((p["label"] for p in PERIOD_OPTIONS if p["id"] == period), period)

            if board_category:
                board_category_label = next(
                    (c["label"] for c in BOARD_CATEGORY_OPTIONS if c["id"] == board_category),
                    board_category,
                )
            else:
                board_category_label = "（カテゴリ指定なし）"

            board_label = ""
            if board_category and board_id:
                for b in board_options:
                    if b["id"] == board_id:
                        board_label = b["label"]
                        break

            key = f"{area}|{period}|{board_category}|{board_id}|{keyword}"
            entry = {
                "key": key,
                "area": area,
                "area_label": area_label,
                "period": period,
                "period_label": period_label,
                "board_category": board_category,
                "board_category_label": board_category_label,
                "board_id": board_id,
                "board_label": board_label,
                "keyword": keyword,
            }
            if not any(e["key"] == key for e in EXTERNAL_SEARCHES):
                EXTERNAL_SEARCHES.append(entry)

    recent_external_searches = list(EXTERNAL_SEARCHES)[::-1]

    try:
        saved_flag = request.query_params.get("saved")
    except Exception:
        saved_flag = None
    if saved_flag and not error_message:
        error_message = "スレッドを保存しました。"

    return templates.TemplateResponse(
        "thread_search.html",
        {
            "request": request,
            "area_options": AREA_OPTIONS,
            "period_options": PERIOD_OPTIONS,
            "board_category_options": BOARD_CATEGORY_OPTIONS,
            "current_area": area,
            "current_period": period,
            "keyword": keyword,
            "results": results,
            "error_message": error_message,
            "board_options": board_options,
            "current_board_category": board_category,
            "current_board_id": board_id,
            "recent_external_searches": recent_external_searches,
            "board_master_json": json.dumps(BOARD_MASTER, ensure_ascii=False),
            "ranking_board": ranking_board,
            "ranking_board_label": ranking_board_label,
            "ranking_source_url": ranking_source_url,
        },
    )


@router.api_route("/thread_search/save", methods=["GET", "POST"])
async def save_external_thread(
    request: Request,
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/thread_search"
    if back_url and "/thread_search/posts" in back_url:
        back_url = "/thread_search"

    url = ""
    saved_ok = False

    try:
        if request.method == "POST":
            form = await request.form()
            thread_url = (form.get("thread_url") or "").strip()
            selected_thread = (form.get("selected_thread") or "").strip()
            url = thread_url or selected_thread
        else:
            params = request.query_params
            thread_url = (params.get("thread_url") or "").strip()
            selected_thread = (params.get("selected_thread") or "").strip()
            url = thread_url or selected_thread
    except Exception:
        url = ""

    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        # ここは既存の /admin/fetch 相当の保存処理に寄せたいなら後で統合できます
        from app.routers.admin import fetch_thread_into_db
        fetch_thread_into_db(db, url)
        saved_ok = True
    except Exception:
        db.rollback()

    redirect_to = _add_flag_to_url(back_url, "saved") if saved_ok else back_url
    return RedirectResponse(url=redirect_to, status_code=303)


@router.post("/thread_search/history/delete")
def delete_external_history(
    request: Request,
    key: str = Form(""),
):
    back_url = request.headers.get("referer") or "/thread_search"
    key = (key or "").strip()
    if not key:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        remaining = [e for e in EXTERNAL_SEARCHES if e.get("key") != key]
        EXTERNAL_SEARCHES.clear()
        EXTERNAL_SEARCHES.extend(remaining)
    except Exception:
        pass

    return RedirectResponse(url=back_url, status_code=303)


@router.post("/thread_search/history/clear")
def clear_external_history(request: Request):
    back_url = request.headers.get("referer") or "/thread_search"
    try:
        EXTERNAL_SEARCHES.clear()
    except Exception:
        pass
    return RedirectResponse(url=back_url, status_code=303)


@router.get("/thread_search/showall", response_class=HTMLResponse)
def thread_showall_page(
    request: Request,
    url: str = "",
    area: str = "7",
    period: str = "3m",
    title_keyword: str = "",
    db: Session = Depends(get_db),
):
    url = (url or "").strip()
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"
    title_keyword = (title_keyword or "").strip()

    error_message = ""
    thread_title_display = ""
    posts_sorted: List[object] = []

    if not url:
        error_message = "URLが指定されていません。"
    elif not _is_valid_bakusai_thread_url(url):
        error_message = "爆サイのスレURLのみ表示できます。"
    else:
        try:
            try:
                t = get_thread_title(url)
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

            all_posts = get_thread_posts_cached(db, url)

            def _post_key(p):
                return p.post_no if getattr(p, "post_no", None) is not None else 10**9

            posts_sorted = sorted(list(all_posts), key=_post_key)
        except Exception as e:
            error_message = f"全レス取得中にエラーが発生しました: {e}"
            posts_sorted = []

    return templates.TemplateResponse(
        "thread_showall.html",
        {
            "request": request,
            "thread_url": url,
            "thread_title": thread_title_display,
            "area": area,
            "period": period,
            "title_keyword": title_keyword,
            "posts": posts_sorted,
            "error_message": error_message,
        },
    )


@router.post("/thread_search/posts", response_class=HTMLResponse)
def thread_search_posts(
    request: Request,
    selected_thread: str = Form(""),
    title_keyword: str = Form(""),
    post_keyword: str = Form(""),
    area: str = Form("7"),
    period: str = Form("3m"),
    board_category: str = Form(""),
    board_id: str = Form(""),
    db: Session = Depends(get_db),
):
    selected_thread = (selected_thread or "").strip()
    title_keyword = (title_keyword or "").strip()
    post_keyword = (post_keyword or "").strip()
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"

    entries: List[dict] = []
    error_message = ""
    thread_title_display: str = ""
    prev_thread_url: Optional[str] = None
    next_thread_url: Optional[str] = None

    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()
    board_category_label: str = ""
    board_label: str = ""
    store_base_title: str = ""

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    else:
        try:
            try:
                t = get_thread_title(selected_thread)
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

            store_base_title = build_store_search_title(thread_title_display or title_keyword)

            if board_category:
                for c in BOARD_CATEGORY_OPTIONS:
                    if c["id"] == board_category:
                        board_category_label = c["label"]
                        break

            if board_category and board_id:
                for b in get_board_options_for_category(board_category):
                    if b["id"] == board_id:
                        board_label = b["label"]
                        break

            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread, area)

            all_posts = get_thread_posts_cached(db, selected_thread)

            def _post_key(p):
                return p.post_no if getattr(p, "post_no", None) is not None else 10**9

            all_posts_sorted = sorted(list(all_posts), key=_post_key)

            posts_by_no: Dict[int, object] = {}
            for p in all_posts_sorted:
                if p.post_no is not None and p.post_no not in posts_by_no:
                    posts_by_no[p.post_no] = p

            replies: Dict[int, List[object]] = defaultdict(list)
            for p in all_posts_sorted:
                if not getattr(p, "anchors", None):
                    continue
                for a in p.anchors:
                    replies[a].append(p)

            def build_reply_tree_external(root) -> List[dict]:
                result: List[dict] = []
                visited: set[int] = set()

                def dfs(post, depth: int):
                    pid = id(post)
                    if pid in visited:
                        return
                    visited.add(pid)
                    if post is not root:
                        result.append({"post": post, "depth": depth})
                    if post.post_no is None:
                        return
                    for child in replies.get(post.post_no, []):
                        dfs(child, depth + 1)

                if root.post_no is not None:
                    for child in replies.get(root.post_no, []):
                        dfs(child, 0)
                return result

            post_keyword_norm = normalize_for_search(post_keyword)

            for root in all_posts_sorted:
                body = root.body or ""
                body_norm = normalize_for_search(body)
                if post_keyword_norm not in body_norm:
                    continue

                context_posts: List[object] = []
                if root.post_no is not None:
                    start_no = max(1, root.post_no - 5)
                    end_no = root.post_no + 5
                    for p in all_posts_sorted:
                        if p.post_no is None:
                            continue
                        if start_no <= p.post_no <= end_no:
                            context_posts.append(p)

                tree_items = build_reply_tree_external(root)

                anchor_targets: List[object] = []
                if getattr(root, "anchors", None):
                    for n in root.anchors:
                        target = posts_by_no.get(n)
                        if target:
                            anchor_targets.append(target)

                entries.append(
                    {
                        "root": root,
                        "context": context_posts,
                        "tree": tree_items,
                        "anchor_targets": anchor_targets,
                    }
                )

        except Exception as e:
            error_message = f"スレッド内検索中にエラーが発生しました: {e}"
            entries = []

    return templates.TemplateResponse(
        "thread_search_posts.html",
        {
            "request": request,
            "thread_url": selected_thread,
            "thread_title": thread_title_display,
            "title_keyword": title_keyword,
            "post_keyword": post_keyword,
            "area": area,
            "period": period,
            "entries": entries,
            "error_message": error_message,
            "highlight": highlight_text,
            "prev_thread_url": prev_thread_url,
            "next_thread_url": next_thread_url,
            "highlight_with_links": highlight_with_links,
            "board_category": board_category,
            "board_id": board_id,
            "board_category_label": board_category_label,
            "board_label": board_label,
            "store_base_title": store_base_title,
        },
    )
