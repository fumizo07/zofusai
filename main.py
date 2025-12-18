# main.py
import os
import re
import secrets
import json
from typing import List, Optional, Dict
from collections import defaultdict, deque
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from constants import (
    THREAD_CACHE_TTL,
    MAX_CACHED_THREADS,
    AREA_OPTIONS,
    BOARD_CATEGORY_OPTIONS,
    BOARD_MASTER,
    PERIOD_OPTIONS,
    get_period_days,
    get_board_options_for_category,
)

from db import engine, Base, get_db
from models import ThreadPost, ThreadMeta
from utils import (
    normalize_for_search,
    highlight_text,
    simplify_thread_title,
    build_store_search_title,
    parse_anchors_csv,
    highlight_with_links,
    build_google_site_search_url,
)

from services import (
    fetch_thread_into_db,
    search_threads_external,
    find_prev_next_thread_urls,
    get_thread_posts_cached,
    is_valid_bakusai_thread_url,
    cleanup_thread_posts_duplicates,
    backfill_posted_at_dt,
)

from scraper import ScrapingError
from ranking import get_board_ranking, RANKING_URL_TEMPLATE


# =========================
# BASIC 認証
# =========================
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)


def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
    if not BASIC_ENABLED:
        return
    correct_username = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    correct_password = secrets.compare_digest(credentials.password, BASIC_AUTH_PASS)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,
    redoc_url=None,
)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ★ツールチップ用
from preview_api import preview_api
app.include_router(preview_api)


RECENT_SEARCHES = deque(maxlen=5)
EXTERNAL_SEARCHES = deque(maxlen=15)


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

    # 既存テーブルに列追加（なければ）
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))

        # ★ 追加：posted_at_dt（DateTime）列
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS posted_at_dt TIMESTAMP"))

        # ★ 追加：Postgresなら部分ユニークインデックス（post_no NULL は除外）
        # 失敗しても握りつぶす（SQLite等環境差を吸収）
        try:
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_thread_posts_url_postno "
                    "ON thread_posts(thread_url, post_no) WHERE post_no IS NOT NULL"
                )
            )
        except Exception:
            pass

    # ★ ⑤：重複掃除＆ posted_at_dt バックフィル（安全側に try）
    try:
        db = next(get_db())
        cleanup_thread_posts_duplicates(db)
        backfill_posted_at_dt(db, limit=10000)
    except Exception:
        pass


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"


# =========================
# 共通：URLメッセージ生成（fetch_next の結果）
# =========================
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


# =========================
# トップページ（内部検索）
# =========================
@app.get("/", response_class=HTMLResponse)
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

    info_message = _get_next_thread_message(request)

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
                        store_title = build_store_search_title(title)

                        block = {
                            "thread_url": thread_url,
                            "thread_title": title,
                            "store_title": store_title,
                            # ★ ③：店舗検索リンク（Cityheaven / DTO）
                            "store_cityheaven_url": build_google_site_search_url("cityheaven.net", store_title),
                            "store_dto_url": build_google_site_search_url("dto.jp", store_title),
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
            # ★ ①：次スレ結果メッセージ
            "info_message": info_message,
        },
    )


# =========================
# 管理用：スレ取り込み画面
# =========================
@app.get("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_get(request: Request, url: str = ""):
    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": url or "",
            "imported": None,
            "error": "",
        },
    )


@app.post("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_post(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    imported: Optional[int] = None
    error: str = ""
    url = (url or "").strip()

    if url:
        # ★ ④：SSRFチェック
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
        {
            "request": request,
            "url": url,
            "imported": imported,
            "error": error,
        },
    )


@app.post("/admin/refetch")
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


@app.post("/admin/delete_thread")
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
        db.query(ThreadPost).filter(ThreadPost.thread_url == url).delete(
            synchronize_session=False
        )
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


# =========================
# スレッド一覧ダッシュボード (/threads)
# =========================
@app.get("/threads", response_class=HTMLResponse)
def list_threads(
    request: Request,
    db: Session = Depends(get_db),
):
    rows = (
        db.query(
            ThreadPost.thread_url,
            func.max(ThreadPost.post_no).label("max_no"),
            func.max(ThreadPost.posted_at_dt).label("last_posted_at_dt"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
        )
        .group_by(ThreadPost.thread_url)
        .order_by(func.max(ThreadPost.posted_at_dt).desc().nullslast(), func.max(ThreadPost.id).desc())
        .all()
    )

    urls = [r.thread_url for r in rows]
    meta_map: Dict[str, ThreadMeta] = {}
    if urls:
        metas = db.query(ThreadMeta).filter(ThreadMeta.thread_url.in_(urls)).all()
        meta_map = {m.thread_url: m for m in metas}

    threads = []
    for r in rows:
        label = meta_map.get(r.thread_url).label if r.thread_url in meta_map else ""
        threads.append(
            {
                "thread_url": r.thread_url,
                "thread_title": simplify_thread_title(r.thread_title or r.thread_url),
                "max_no": r.max_no,
                "last_posted_at": r.last_posted_at_dt,
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
    info_message = _get_next_thread_message(request)

    return templates.TemplateResponse(
        "threads.html",
        {
            "request": request,
            "threads": threads,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            # ★ ①：次スレ結果メッセージ
            "info_message": info_message,
        },
    )


@app.post("/threads/label")
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


# =========================
# 外部スレッド検索（爆サイ）
# =========================
@app.get("/thread_search", response_class=HTMLResponse)
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
            area_label = next(
                (a["label"] for a in AREA_OPTIONS if a["code"] == area),
                area,
            )
            period_label = next(
                (p["label"] for p in PERIOD_OPTIONS if p["id"] == period),
                period,
            )
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


def _add_flag_to_url(back_url: str, key: str) -> str:
    if not back_url:
        return f"/?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


# =========================
# 外部スレッド → DB 保存
# =========================
@app.api_route("/thread_search/save", methods=["GET", "POST"])
async def save_external_thread(
    request: Request,
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/thread_search"
    if back_url and "/thread_search/posts" in back_url:
        back_url = "/thread_search"

    url = ""

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

    # ★ ④：SSRFチェック
    if not is_valid_bakusai_thread_url(url):
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
        redirect_to = _add_flag_to_url(back_url, "saved")
    except Exception:
        db.rollback()
        redirect_to = back_url

    return RedirectResponse(url=redirect_to, status_code=303)


# =========================
# 次スレ取得
# =========================
@app.post("/admin/fetch_next")
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


# =========================
# 外部スレッド全レス表示
# =========================
@app.get("/thread_search/showall", response_class=HTMLResponse)
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
    elif not is_valid_bakusai_thread_url(url):
        error_message = "爆サイのスレURLのみ表示できます。"
    else:
        try:
            try:
                t = get_thread_title(url)  # type: ignore[name-defined]
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

    # ★ ③：店舗検索（DTO/Cityheaven）
    store_title = build_store_search_title(thread_title_display or title_keyword)
    store_cityheaven_url = build_google_site_search_url("cityheaven.net", store_title)
    store_dto_url = build_google_site_search_url("dto.jp", store_title)

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
            "store_title": store_title,
            "store_cityheaven_url": store_cityheaven_url,
            "store_dto_url": store_dto_url,
        },
    )


# =========================
# 外部スレッド内検索
# =========================
@app.post("/thread_search/posts", response_class=HTMLResponse)
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
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    entries: List[dict] = []
    error_message = ""
    thread_title_display: str = ""
    prev_thread_url: Optional[str] = None
    next_thread_url: Optional[str] = None

    board_category_label: str = ""
    board_label: str = ""

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    elif not is_valid_bakusai_thread_url(selected_thread):
        error_message = "爆サイのスレURLのみ検索できます。"
    else:
        try:
            try:
                t = get_thread_title(selected_thread)  # type: ignore[name-defined]
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

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

            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread)

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

    # ★ ③：店舗検索（DTO/Cityheaven）
    store_base_title = build_store_search_title(thread_title_display or title_keyword)
    store_cityheaven_url = build_google_site_search_url("cityheaven.net", store_base_title)
    store_dto_url = build_google_site_search_url("dto.jp", store_base_title)

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
            "store_cityheaven_url": store_cityheaven_url,
            "store_dto_url": store_dto_url,
        },
    )
