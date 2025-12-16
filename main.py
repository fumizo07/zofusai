
import os
import re
import secrets
import json
from collections import defaultdict, deque
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from sqlalchemy import func, text as sa_text
from sqlalchemy.orm import Session

# constants.py から読み込み（UI用・検索条件用）
from constants import (
    AREA_OPTIONS,
    BOARD_CATEGORY_OPTIONS,
    BOARD_MASTER,
    PERIOD_OPTIONS,
    get_period_days,
    get_board_options_for_category,
)

# db.py から読み込み
from db import engine, Base, get_db

# models.py から読み込み
from models import ThreadPost, ThreadMeta

# utils.py から読み込み
from utils import (
    normalize_for_search,
    highlight_text,
    simplify_thread_title,
    build_store_search_title,
    parse_anchors_csv,
    highlight_with_links,
    parse_posted_at_value,
)

# scraper.py（タイトル表示用）
from scraper import ScrapingError, get_thread_title

# services.py（集約した処理）
from services import (
    fetch_thread_into_db,
    search_threads_external,
    find_prev_next_thread_urls,
    get_thread_posts_cached,
    is_valid_bakusai_thread_url,
)

# ranking.py（外部検索UI用）
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

# 静的ファイル（CSS 等）
app.mount("/static", StaticFiles(directory="static"), name="static")

# 最近の検索条件（メモリ上）
RECENT_SEARCHES = deque(maxlen=5)
EXTERNAL_SEARCHES = deque(maxlen=15)


@app.on_event("startup")
def on_startup():
    # テーブル作成
    Base.metadata.create_all(bind=engine)

    # 既存テーブルに列追加（なければ）
    with engine.begin() as conn:
        conn.execute(sa_text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(sa_text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(sa_text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))

        # ⑤ 余裕があれば：DB制約（重複防止）
        # 既存データに重複があると失敗するので、失敗してもアプリは落とさない方針。
        # Postgres想定：部分ユニーク（post_no が NULL の行は除外）
        try:
            conn.execute(
                sa_text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_thread_posts_url_postno "
                    "ON thread_posts (thread_url, post_no) "
                    "WHERE post_no IS NOT NULL"
                )
            )
        except Exception:
            # SQLite等や既存重複で失敗する可能性あり。ここでは握りつぶす（ログ基盤があるなら logging 推奨）
            pass


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"


# =========================
# テキスト整形・検索用ユーティリティ
# =========================
def build_reply_tree(all_posts: List["ThreadPost"], root: "ThreadPost") -> List[dict]:
    """
    root（ヒットしたレス）にぶら下がる返信ツリーを構築
    """
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

    try:
        # 全タグの集計（タグ一覧用）
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

        # 検索条件が入力されている場合のみ検索を実行
        if keyword_raw or thread_filter_raw or tags_input_raw:
            # 最近の検索条件をメモリに保存
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

                    # 前後 5 レスのコンテキスト
                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts_thread:
                        start_no = max(1, root.post_no - 5)
                        end_no = root.post_no + 5
                        context_posts = [
                            p
                            for p in all_posts_thread
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    # ツリー表示用
                    tree_items = build_reply_tree(all_posts_thread, root)

                    # アンカー先
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


# =========================
# JSON API（簡易）
# =========================
@app.get("/api/search")
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

    # ④ SSRF対策
    if url and not is_valid_bakusai_thread_url(url):
        error = "爆サイのスレURL（/thr_res/ または /thr_res_show/）のみ取り込みできます。"
        return templates.TemplateResponse(
            "fetch.html",
            {"request": request, "url": url, "imported": None, "error": error},
        )

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

    # ④ SSRF対策
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
            func.max(ThreadPost.posted_at).label("last_posted_at"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
        )
        .group_by(ThreadPost.thread_url)
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

    # ⑤ posted_at が Text のままでも “正しく並べる” 対策（Python側で解釈してソート）
    def _thread_sort_key(t: dict):
        dt = parse_posted_at_value(t.get("last_posted_at") or "")
        # dt が取れないものは最古扱い
        return dt or datetime(1970, 1, 1)

    threads.sort(key=_thread_sort_key, reverse=True)

    # タグ一覧
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
# 投稿単位のタグ・メモ編集
# =========================
@app.get("/post/{post_id}/edit", response_class=HTMLResponse)
def edit_post_get(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
):
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    return templates.TemplateResponse(
        "edit_post.html",
        {
            "request": request,
            "post": post,
        },
    )


@app.post("/post/{post_id}/edit")
def edit_post_post(
    request: Request,
    post_id: int,
    tags: str = Form(""),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    if post:
        post.tags = (tags or "").strip() or None
        post.memo = (memo or "").strip() or None
        db.commit()

    back_url = request.headers.get("referer") or "/"
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
        return f"/thread_search?{key}=1"
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

    # ④ SSRF対策
    if not is_valid_bakusai_thread_url(url):
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
        saved_ok = True
    except Exception:
        db.rollback()

    redirect_to = _add_flag_to_url(back_url, "saved") if saved_ok else back_url
    return RedirectResponse(url=redirect_to, status_code=303)


# =========================
# 次スレ取得（保存スレ・内部検索共通で使える）
# =========================
@app.post("/admin/fetch_next")
def fetch_next_thread(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/threads"
    url = (url or "").strip()

    if not url:
        redirect_to = _add_flag_to_url(back_url, "next_error")
        return RedirectResponse(url=redirect_to, status_code=303)

    # ④ SSRF対策
    if not is_valid_bakusai_thread_url(url):
        redirect_to = _add_flag_to_url(back_url, "next_error")
        return RedirectResponse(url=redirect_to, status_code=303)

    _, next_url = find_prev_next_thread_urls(url, "")
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
# 外部検索履歴の削除
# =========================
@app.post("/thread_search/history/delete")
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


@app.post("/thread_search/history/clear")
def clear_external_history(request: Request):
    back_url = request.headers.get("referer") or "/thread_search"
    try:
        EXTERNAL_SEARCHES.clear()
    except Exception:
        pass
    return RedirectResponse(url=back_url, status_code=303)


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
    store_base_title: str = ""

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    elif not is_valid_bakusai_thread_url(selected_thread):
        # ④ SSRF対策
        error_message = "爆サイのスレURLのみ検索できます。"
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
