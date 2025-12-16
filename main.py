import os
import re
import unicodedata
import secrets
import json
from typing import List, Optional, Dict
from collections import defaultdict, deque
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlencode
from urllib.parse import urlparse
from types import SimpleNamespace
from sqlalchemy import DateTime, UniqueConstraint

# constants.pyから読み込み
from constants import THREAD_CACHE_TTL, MAX_CACHED_THREADS, AREA_OPTIONS, BOARD_CATEGORY_OPTIONS, BOARD_MASTER, PERIOD_OPTIONS, get_period_days, get_board_options_for_category

# db.pyから読み込み
from db import engine, Base, get_db

# models.pyから読み込み
from models import ThreadPost, ThreadMeta, CachedThread, CachedPost

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles  # 追加

from sqlalchemy import Column, Integer, Text, func, text
from sqlalchemy.orm import Session

from markupsafe import Markup, escape

from scraper import fetch_posts_from_thread, ScrapingError, get_thread_title

# ランキング（外部検索用）で使うのは後ろの thread_search_page 側だけ
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
# テキスト整形・検索用ユーティリティ
# =========================
def _normalize_lines(text_value: str) -> str:
    """
    余計な行頭全角スペース・空行を削除して、見やすい形に整える
    """
    lines = text_value.splitlines()
    cleaned: List[str] = []
    leading = True
    for line in lines:
        # 先頭側の空白行は丸ごと削る
        if leading and line.strip() == "":
            continue
        # 行頭の半角/全角スペースを削る
        line = re.sub(r'^[\s\u3000\xa0]+', '', line)
        cleaned.append(line)
        leading = False
    return "\n".join(cleaned)


def to_hiragana(s: str) -> str:
    result = []
    for ch in s:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            result.append(chr(code - 0x60))
        else:
            result.append(ch)
    return "".join(result)


def to_katakana(s: str) -> str:
    result = []
    for ch in s:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            result.append(chr(code + 0x60))
        else:
            result.append(ch)
    return "".join(result)


# ★追加：小書き母音を通常の「あいうえお」に揃えるマップ
SMALL_KANA_MAP = str.maketrans(
    {
        "ぁ": "あ",
        "ぃ": "い",
        "ぅ": "う",
        "ぇ": "え",
        "ぉ": "お",
    }
)

def normalize_for_search(s: Optional[str]) -> str:
    """
    検索用の正規化：
    - NFKC
    - カタカナ → ひらがな
    - 小書き母音（ぁぃぅぇぉ）を通常のあいうえおに揃える
    - 小文字化
    """
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = to_hiragana(s)
    s = s.translate(SMALL_KANA_MAP)
    s = s.lower()
    return s



def _build_highlight_variants(keyword: str) -> List[str]:
    """
    強調表示用のバリアント生成：
    - NFKC
    - ひらがな / カタカナ両対応
    - 小書き母音（ぁぃぅぇぉ）を通常のあいうえおに揃えた形も含める
    """
    if not keyword:
        return []
    base = unicodedata.normalize("NFKC", keyword)
    hira = to_hiragana(base)
    kata = to_katakana(hira)

    raw_variants = {base, hira, kata}
    expanded: set[str] = set()
    for v in raw_variants:
        if not v:
            continue
        expanded.add(v)
        expanded.add(v.translate(SMALL_KANA_MAP))

    variants = {v for v in expanded if v}
    return sorted(variants, key=len, reverse=True)



def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文の中でキーワード部分を <mark> で囲って強調表示
    （ひらがな/カタカナ両方ヒット）
    """
    if text_value is None:
        text_value = ""
    text_value = _normalize_lines(text_value)
    if not keyword:
        return Markup(escape(text_value))

    escaped = escape(text_value)
    variants = _build_highlight_variants(keyword)
    if not variants:
        return Markup(escaped)

    try:
        pattern = re.compile("(" + "|".join(re.escape(v) for v in variants) + ")", re.IGNORECASE)
    except re.error:
        return Markup(escaped)

    def repl(match):
        return Markup(f"<mark>{match.group(0)}</mark>")

    highlighted = pattern.sub(lambda m: repl(m), escaped)
    return Markup(highlighted)


def simplify_thread_title(title: str) -> str:
    if not title:
        return ""
    for sep in ["｜", "|", " - "]:
        if sep in title:
            title = title.split(sep)[0]
    return title.strip()

# ★ここから追加：店舗ページ検索用のタイトル整形
_EMOJI_PATTERN = re.compile(
    "["  # ざっくり emoji / 記号レンジ
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "]"
)


def remove_emoji(text: str) -> str:
    return _EMOJI_PATTERN.sub("", text or "")


def build_store_search_title(title: str) -> str:
    """
    店舗ページ検索用：
    - 絵文字を削除
    - 末尾の「★12」「 12」などのスレ番を削る
    """
    if not title:
        return ""
    t = simplify_thread_title(title)
    t = remove_emoji(t)
    # 末尾の記号＋数字だけをざっくり落とす（★12 / 12 / ★ 12 など）
    t = re.sub(r"[\s　]*[★☆◇◆◎○●⚫⚪※✕✖️✖︎-]*\s*\d{1,3}\s*$", "", t)
    return t.strip()
# ★ここまで追加

def parse_anchors_csv(s: Optional[str]) -> List[int]:
    if not s:
        return []
    nums: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if part.isdigit():
            nums.append(int(part))
    return sorted(set(nums))


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


def parse_posted_at_value(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

def _evict_old_cached_threads(db: Session) -> None:
    """
    キャッシュが増えすぎたら、最終アクセスが古いスレから削除する。
    """
    try:
        cnt = db.query(func.count(CachedThread.thread_url)).scalar() or 0
        if cnt <= MAX_CACHED_THREADS:
            return

        over = cnt - MAX_CACHED_THREADS
        old_threads = (
            db.query(CachedThread)
            .order_by(CachedThread.last_accessed_at.asc())
            .limit(over)
            .all()
        )

        for t in old_threads:
            db.query(CachedPost).filter(CachedPost.thread_url == t.thread_url).delete(synchronize_session=False)
            db.query(CachedThread).filter(CachedThread.thread_url == t.thread_url).delete(synchronize_session=False)

        db.commit()
    except Exception:
        db.rollback()


def _save_thread_posts_to_cache(db: Session, thread_url: str, posts: List[object]) -> None:
    """
    scraper.fetch_posts_from_thread() の結果を、キャッシュDBに保存する。
    （安全優先：一旦そのスレのキャッシュを全削除→入れ直し）
    """
    now = datetime.utcnow()

    # スレ分を全消し→入れ直し（堅牢だがシンプル）
    db.query(CachedPost).filter(CachedPost.thread_url == thread_url).delete(synchronize_session=False)

    bulk = []
    for p in posts:
        body = (getattr(p, "body", None) or "").strip()
        if not body:
            continue

        post_no = getattr(p, "post_no", None)
        posted_at = getattr(p, "posted_at", None)

        # anchors は list[int] 想定（scraperの構造）
        anchors_list = getattr(p, "anchors", None)
        if anchors_list:
            anchors_str = "," + ",".join(str(a) for a in anchors_list) + ","
        else:
            anchors_str = None

        bulk.append(
            CachedPost(
                thread_url=thread_url,
                post_no=post_no,
                posted_at=posted_at,
                body=body,
                anchors=anchors_str,
            )
        )

    if bulk:
        db.bulk_save_objects(bulk)

    meta = db.query(CachedThread).filter(CachedThread.thread_url == thread_url).first()
    if not meta:
        meta = CachedThread(
            thread_url=thread_url,
            fetched_at=now,
            last_accessed_at=now,
        )
        db.add(meta)
    else:
        meta.fetched_at = now
        meta.last_accessed_at = now

    db.commit()

    # 上限超えたら削除
    _evict_old_cached_threads(db)


def _load_thread_posts_from_cache(db: Session, thread_url: str) -> List[CachedPost]:
    return (
        db.query(CachedPost)
        .filter(CachedPost.thread_url == thread_url)
        .order_by(CachedPost.post_no.asc().nullslast(), CachedPost.id.asc())
        .all()
    )


def get_thread_posts_cached(db: Session, thread_url: str) -> List[object]:
    """
    外部検索用：
    - キャッシュが新しければDBから返す
    - 古い/無ければWebから取得してDB保存して返す
    返すのは「scraperのpostっぽいオブジェクト」（templatesが壊れない形）
    """
    thread_url = (thread_url or "").strip()
    if not thread_url:
        return []

    now = datetime.utcnow()
    meta = db.query(CachedThread).filter(CachedThread.thread_url == thread_url).first()

    need_refresh = True
    if meta:
        # TTL内なら refreshしない
        if now - meta.fetched_at < THREAD_CACHE_TTL:
            need_refresh = False

    if need_refresh:
        posts = fetch_posts_from_thread(thread_url)
        _save_thread_posts_to_cache(db, thread_url, list(posts))
        cached_rows = _load_thread_posts_from_cache(db, thread_url)
    else:
        # アクセス時刻更新
        try:
            meta.last_accessed_at = now
            db.commit()
        except Exception:
            db.rollback()
        cached_rows = _load_thread_posts_from_cache(db, thread_url)

    # cached_rows(SQLAlchemy) -> scraperっぽい形へ
    result = []
    for r in cached_rows:
        result.append(
            SimpleNamespace(
                post_no=r.post_no,
                posted_at=r.posted_at,
                body=r.body,
                anchors=parse_anchors_csv(r.anchors),
            )
        )

    return result


def linkify_anchors_in_html(thread_url: str, html: str) -> Markup:
    """
    すでに escape / highlight 済みの HTML 文字列内の「&gt;&gt;数字」を
    レス個別ページへのリンクに変換する。
    """
    if not html:
        return Markup("")

    base = thread_url or ""

    # thr_res / thr_res_show の acode〜tid までをベースURLにする
    m = re.search(
        r"(https://bakusai\.com/thr_res(?:_show)?/acode=\d+/ctgid=\d+/bid=\d+/tid=\d+/)",
        base,
    )
    if m:
        base_rr = m.group(1)
    else:
        base_rr = base

    def repl(match: re.Match) -> str:
        no = match.group(1)
        url = base_rr
        if "thr_res_show" not in url:
            url = url.replace("/thr_res/", "/thr_res_show/")
        if not url.endswith("/"):
            url += "/"
        href = f"{url}rrid={no}/"
        return (
            f'<a href="{href}" target="_blank" '
            f'rel="nofollow noopener noreferrer">&gt;&gt;{no}</a>'
        )

    # highlight_text が escape 済みなので、「>>」は「&gt;&gt;」になっている
    linked = re.sub(r"&gt;&gt;(\d+)", repl, html)
    return Markup(linked)


def highlight_with_links(text_value: Optional[str], keyword: str, thread_url: str) -> Markup:
    """
    1) 検索キーワードのハイライト
    2) >>番号 を個別レスへのリンク化
    をまとめて行う。
    """
    highlighted = highlight_text(text_value, keyword)
    return linkify_anchors_in_html(thread_url, str(highlighted))



# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,   # /docs を封印
    redoc_url=None,  # /redoc も封印
)
templates = Jinja2Templates(directory="templates")

# 静的ファイル（CSS 等）
app.mount("/static", StaticFiles(directory="static"), name="static")  # ★追加

# 最近の検索条件（メモリ上）
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


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"


# =========================
# スレ取り込み共通処理
# =========================
def fetch_thread_into_db(db: Session, url: str) -> int:
    url = (url or "").strip()
    if not url:
        return 0

    last_no = (
        db.query(func.max(ThreadPost.post_no))
        .filter(ThreadPost.thread_url == url)
        .scalar()
    )
    if last_no is None:
        last_no = 0

    # タイトル取得・簡略化
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
            # すでに取り込み済みのレスはスキップ
            continue

        if getattr(sp, "anchors", None):
            anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
        else:
            anchors_str = None

        # すでに同じレスが入っていれば更新のみ
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
                post_no=sp.post_no,
                posted_at=getattr(sp, "posted_at", None),  # ★ここを修正
                body=body,
                anchors=anchors_str,
            )
        )
        count += 1

    db.commit()
    return count


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
                            # ★追加：店舗ページ検索用に整形済みタイトルを持たせる
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
    # スレ単位の集約
    rows = (
        db.query(
            ThreadPost.thread_url,
            func.max(ThreadPost.post_no).label("max_no"),
            func.max(ThreadPost.posted_at).label("last_posted_at"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
        )
        .group_by(ThreadPost.thread_url)
        .order_by(func.max(ThreadPost.posted_at).desc())
        .all()
    )

    urls = [r.thread_url for r in rows]
    meta_map: Dict[str, ThreadMeta] = {}
    if urls:
        metas = db.query(ThreadMeta).filter(ThreadMeta.thread_url.in_(urls)).all()
        meta_map = {m.thread_url: m for m in metas}

    threads = []
    for r in rows:
        label = None
        if r.thread_url in meta_map:
            label = meta_map[r.thread_url].label
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

    # 最近の検索条件
    recent_searches_view = list(RECENT_SEARCHES)[::-1]

    # 次スレ取得結果メッセージ
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
def search_threads_external(
    area_code: str,
    keyword: str,
    max_days: Optional[int],
    board_category: str = "",
    board_id: str = "",
) -> List[dict]:
    """
    爆サイの「スレッド検索」結果から、タイトル一覧を取得する。
    board_category, board_id が指定されていればそれをURLに反映。
    """
    keyword = (keyword or "").strip()
    area_code = (area_code or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    if not area_code or not keyword:
        return []

    # URL 組み立て
    # 例： https://bakusai.com/sch_thr_thread/acode=7/ctgid=103/bid=410/p=1/sch=thr_sch/sch_range=board/word=ピンク/
    base = f"https://bakusai.com/sch_thr_thread/acode={area_code}/"
    if board_category:
        base += f"ctgid={board_category}/"
    if board_id:
        base += f"bid={board_id}/"

    url = (
        base
        + "p=1/sch=thr_sch/sch_range=board/word="
        + quote_plus(keyword)
        + "/"
    )

    resp = requests.get(
        url,
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    threads: List[dict] = []

    threshold: Optional[datetime] = None
    if max_days is not None:
        threshold = datetime.now() - timedelta(days=max_days)

    keyword_norm = normalize_for_search(keyword)

    # 「最新レス投稿日時」テキストを手掛かりにスレブロックを探す
    for s in soup.find_all(string=re.compile("最新レス投稿日時")):
        text = str(s)
        m = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", text)
        if not m:
            continue
        try:
            dt = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M")
        except ValueError:
            continue

        if threshold is not None and dt < threshold:
            continue

        parent = s.parent
        link = None
        # 近くの /thr_res/ へのリンクを探す
        while parent is not None and parent.name not in ("html", "body"):
            candidate = parent.find("a", href=True)
            if candidate and "/thr_res/" in candidate.get("href", ""):
                link = candidate
                break
            parent = parent.parent

        if not link:
            continue

        title = (link.get_text() or "").strip()
        if not title:
            continue

        title_norm = normalize_for_search(title)
        if keyword_norm not in title_norm:
            continue

        href = link.get("href", "")
        if not href:
            continue

        if href.startswith("//"):
            full_url = "https:" + href
        elif href.startswith("/"):
            full_url = "https://bakusai.com" + href
        else:
            full_url = href

        threads.append(
            {
                "title": title,
                "url": full_url,
                "last_post_at_str": dt.strftime("%Y-%m-%d %H:%M"),
            }
        )

    # URL でユニーク化
    unique_by_url: Dict[str, dict] = {}
    for t in threads:
        if t["url"] not in unique_by_url:
            unique_by_url[t["url"]] = t

    result = list(unique_by_url.values())
    result.sort(key=lambda x: x.get("last_post_at_str") or "", reverse=True)
    return result


@app.get("/thread_search", response_class=HTMLResponse)
def thread_search_page(
    request: Request,
    area: str = "7",
    period: str = "3m",
    keyword: str = "",
    board_category: str = "103",
    board_id: str = "5922",
):
    # パラメータ整形
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"
    keyword = (keyword or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    results: List[dict] = []
    error_message = ""

    # ★ ランキング結果（板ごと）
    ranking_board = None
    ranking_board_label = ""
    ranking_source_url = ""

    # 板リストをカテゴリから取得（履歴のラベル用にも使う）
    board_options = get_board_options_for_category(board_category)

    # 検索実行
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

        # ★ ランキング取得（検索が成功していて、板が指定されているときだけ）
        if not error_message and board_category and board_id:
            # 表示用ラベル（「大阪デリヘル・お店」など）を取得
            board_label = ""
            for b in board_options:
                if b["id"] == board_id:
                    board_label = b["label"]
                    break

            ranking_board_label = board_label or "選択した板"
            ranking_board = get_board_ranking(area, board_category, board_id)

            # ★ 爆サイ側のランキング元ページURL（thr_tl）を組み立て
            if ranking_board:
                ranking_source_url = RANKING_URL_TEMPLATE.format(
                    acode=area,
                    ctgid=board_category,
                    bid=board_id,
                )

        # 検索履歴を追加（エラーが出ていないときだけ）
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

    # スレ保存完了フラグ（/thread_search/save からのリダイレクト時）
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
            # ★ 追加
            "ranking_board": ranking_board,
            "ranking_board_label": ranking_board_label,
            "ranking_source_url": ranking_source_url,
        },
    )


def _add_flag_to_url(back_url: str, key: str) -> str:
    """
    クエリに {key}=1 を付与するヘルパー。
    すでに付いている場合はそのまま返す。
    """
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
    """
    外部スレッド検索・スレッド内検索結果から
    「このスレを保存」で呼び出されるエンドポイント。

    - GET / POST どちらで来ても対応
    - thread_url / selected_thread のどちらかに URL が入っていれば保存
    """
    back_url = request.headers.get("referer") or "/thread_search"

    # スレ内検索結果（/thread_search/posts）から呼ばれた場合、
    # そのまま戻すと GET /thread_search/posts になり 405 になるので
    # 安全な /thread_search に退避させる。
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
        fetch_thread_into_db(db, url)
        saved_ok = True
    except Exception:
        db.rollback()

    if saved_ok:
        redirect_to = _add_flag_to_url(back_url, "saved")
    else:
        redirect_to = back_url

    return RedirectResponse(url=redirect_to, status_code=303)


# =========================
# 次スレ取得（保存スレ・内部検索共通で使える）
# =========================
def _normalize_bakusai_href(href: str) -> str:
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://bakusai.com" + href
    return href


def find_prev_next_thread_urls(thread_url: str, area_code: str) -> tuple[Optional[str], Optional[str]]:
    try:
        resp = requests.get(
            thread_url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
    except Exception:
        return (None, None)

    soup = BeautifulSoup(resp.text, "html.parser")
    pager = soup.find("div", id="thr_pager")
    if not pager:
        return (None, None)

    prev_div = pager.find("div", class_="sre_mae")
    next_div = pager.find("div", class_="sre_tsugi")

    def pick_url(div) -> Optional[str]:
        if not div:
            return None
        a = div.find("a", href=True)
        if not a:
            return None
        href = a.get("href", "")
        if not href:
            return None
        return _normalize_bakusai_href(href)

    prev_url = pick_url(prev_div)
    next_url = pick_url(next_div)
    return (prev_url, next_url)


@app.post("/admin/fetch_next")
def fetch_next_thread(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    取り込み済みスレの「次スレ」を取得して DB に保存する。
    呼び出し元（/threads や /）にリダイレクトし、クエリパラメータで結果を返す。
    """
    back_url = request.headers.get("referer") or "/threads"
    url = (url or "").strip()

    if not url:
        redirect_to = _add_flag_to_url(back_url, "next_error")
        return RedirectResponse(url=redirect_to, status_code=303)

    # ページャーから次スレ URL を取得
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
# 外部スレッド内検索
# =========================
def _is_valid_bakusai_thread_url(u: str) -> bool:
    """
    SSRF対策：取得対象URLを爆サイのスレURLに限定する
    """
    try:
        p = urlparse(u)
    except Exception:
        return False

    if p.scheme not in ("http", "https"):
        return False

    host = (p.netloc or "").lower()
    if host not in ("bakusai.com", "www.bakusai.com"):
        return False

    path = p.path or ""
    # スレ本体（/thr_res/）だけ許可（必要なら後で緩められます）
    if "/thr_res/" not in path:
        return False

    return True

@app.get("/thread_search/showall", response_class=HTMLResponse)
def thread_showall_page(
    request: Request,
    url: str = "",
    area: str = "7",
    period: str = "3m",
    title_keyword: str = "",
    db: Session = Depends(get_db),  # ★これを追加
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


@app.post("/thread_search/posts", response_class=HTMLResponse)
def thread_search_posts(
    request: Request,
    selected_thread: str = Form(""),
    title_keyword: str = Form(""),
    post_keyword: str = Form(""),
    area: str = Form("7"),
    period: str = Form("3m"),
    board_category: str = Form(""),   # ★追加
    board_id: str = Form(""),         # ★追加
    db: Session = Depends(get_db),   # ★これを追加
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
    # ★ 追加: カテゴリ / 板情報
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
            # ★追加：店舗ページ検索用タイトル
            store_base_title = build_store_search_title(thread_title_display or title_keyword)


            # ★ここから追加: カテゴリ / 板のラベル決定
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
            # ★ここまで追加

            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread, area)

            all_posts = get_thread_posts_cached(db, selected_thread)

            # post_no でソートしておく（コンテキスト順がバラつかないように）
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
            # ★追加
            "board_category": board_category,
            "board_id": board_id,
            "board_category_label": board_category_label,
            "board_label": board_label,
            # ★追加
            "store_base_title": store_base_title,
        },
    )
