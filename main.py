import os
import re
from typing import List, Optional, Dict
from collections import defaultdict
from datetime import datetime
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import Column, Integer, Text, create_engine, func, text, or_
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from markupsafe import Markup, escape

from scraper import fetch_posts_from_thread, ScrapingError, get_thread_title

# ====== データベース設定 ======

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が設定されていません。環境変数 DATABASE_URL を確認してください。")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class ThreadPost(Base):
    """
    掲示板のレス1件を表すテーブル。
    """
    __tablename__ = "thread_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)   # 取得元スレURL
    thread_title = Column(Text, nullable=True)              # スレタイトル（簡易版）
    post_no = Column(Integer, nullable=True, index=True)    # レス番号
    posted_at = Column(Text, nullable=True)                 # 投稿日時（文字列）
    body = Column(Text, nullable=False)                     # 本文
    anchors = Column(Text, nullable=True)                   # 「,55,60,130,」のような文字列
    tags = Column(Text, nullable=True)                      # 自分用タグ
    memo = Column(Text, nullable=True)                      # 自分用メモ


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ====== 外部スレッド検索用 定数 ======

# エリアは、とりあえず利用頻度が高そうな関西(7)だけ用意しておく。
# 必要になったら code / label を増やせるようにしておく。
AREA_OPTIONS = [
    {"code": "7", "label": "関西"},
]

PERIOD_OPTIONS = [
    {"id": "7d", "label": "7日以内", "days": 7},
    {"id": "1m", "label": "1ヶ月以内", "days": 31},
    {"id": "3m", "label": "3ヶ月以内", "days": 93},
    {"id": "6m", "label": "6ヶ月以内", "days": 186},
    {"id": "1y", "label": "1年以内", "days": 365},
    {"id": "2y", "label": "2年以内", "days": 730},
]

PERIOD_ID_TO_DAYS = {p["id"]: p["days"] for p in PERIOD_OPTIONS}


def get_period_days(period_id: str) -> Optional[int]:
    """
    期間IDから、何日以内かを返す。
    """
    return PERIOD_ID_TO_DAYS.get(period_id)


# ====== テキスト整形＆ハイライト ======

def _normalize_lines(text_value: str) -> str:
    """
    各行ごとに、先頭の空白・全角スペース・NBSPなどを削る。
    「                        >>251」のような行を「>>251」にする。
    """
    lines = text_value.splitlines()
    cleaned_lines = []
    for line in lines:
        line = re.sub(r'^[\s\u3000\xa0]+', '', line)
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文中の検索語を <mark> で囲んで強調表示する。
    表示の前に行頭スペースを削って整形する。
    """
    if text_value is None:
        text_value = ""

    text_value = _normalize_lines(text_value)

    if not keyword:
        return Markup(escape(text_value))

    escaped = escape(text_value)
    pattern = re.compile(re.escape(keyword))

    def repl(match):
        return Markup(f"<mark>{match.group(0)}</mark>")

    highlighted = pattern.sub(lambda m: repl(m), escaped)
    return Markup(highlighted)


def simplify_thread_title(title: str) -> str:
    """
    ページタイトルからサイト名などの余計な部分をざっくり落とす。
    """
    if not title:
        return ""
    for sep in ["｜", "|", " - "]:
        if sep in title:
            title = title.split(sep)[0]
    return title.strip()


# ====== ツリー構築用のヘルパー ======

def parse_anchors_csv(s: Optional[str]) -> List[int]:
    """
    anchors カラムの文字列（例：",55,60,130,"）から整数リストを取り出す。
    """
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


def build_reply_tree(all_posts: List[ThreadPost], root: ThreadPost) -> List[dict]:
    """
    1スレ内の全レスから、
    「root.post_no にアンカーを飛ばしているレス」を起点に木構造を作る。
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


# ====== アプリ本体 ======

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def on_startup():
    """
    アプリ起動時のテーブル作成＋カラム追加。
    """
    Base.metadata.create_all(bind=engine)

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))


# ====== スレ取り込みロジック共通化 ======

def fetch_thread_into_db(db: Session, url: str) -> int:
    """
    指定URLのスレッドを取得し、DBに保存する共通処理。
    すでに保存されているレス番号まではスキップし、新しい分だけ追加。
    """
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

    thread_title = get_thread_title(url)
    if thread_title:
        thread_title = simplify_thread_title(thread_title)

        # 既存レコードの thread_title が空のものを一括更新
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
            continue

        if sp.anchors:
            anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
        else:
            anchors_str = None

        existing = None
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
            if not existing.posted_at and sp.posted_at:
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
                posted_at=sp.posted_at,
                body=body,
                anchors=anchors_str,
            )
        )
        count += 1

    db.commit()
    return count


# ====== 内部検索（Personal Search のメイン画面） ======

@app.get("/", response_class=HTMLResponse)
def show_search_page(
    request: Request,
    q: str = "",
    thread_filter: str = "",
    tags: str = "",
    tag_mode: str = "or",
    db: Session = Depends(get_db),
):
    keyword = (q or "").strip()
    thread_filter = (thread_filter or "").strip()
    tags_input = (tags or "").strip()
    tag_mode = (tag_mode or "or").lower()

    thread_results: List[dict] = []
    hit_count = 0
    error_message: str = ""

    try:
        if keyword or thread_filter or tags_input:
            query = db.query(ThreadPost)

            if keyword:
                query = query.filter(ThreadPost.body.contains(keyword))

            if thread_filter:
                query = query.filter(
                    or_(
                        ThreadPost.thread_url.contains(thread_filter),
                        ThreadPost.thread_title.contains(thread_filter),
                    )
                )

            tags_list: List[str] = []
            if tags_input:
                tags_list = [t.strip() for t in tags_input.split(",") if t.strip()]

            if tags_list:
                if tag_mode == "and":
                    for t in tags_list:
                        query = query.filter(ThreadPost.tags.contains(t))
                else:
                    or_conditions = [ThreadPost.tags.contains(t) for t in tags_list]
                    query = query.filter(or_(*or_conditions))

            hits: List[ThreadPost] = (
                query.order_by(ThreadPost.thread_url.asc(), ThreadPost.post_no.asc()).all()
            )
            hit_count = len(hits)

            if hits:
                all_posts_by_thread: Dict[str, List[ThreadPost]] = {}
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
                            "entries": [],
                        }
                        thread_map[thread_url] = block
                        thread_results.append(block)

                    if thread_url not in all_posts_by_thread:
                        all_posts = (
                            db.query(ThreadPost)
                            .filter(ThreadPost.thread_url == thread_url)
                            .order_by(ThreadPost.post_no.asc())
                            .all()
                        )
                        all_posts_by_thread[thread_url] = all_posts
                    else:
                        all_posts = all_posts_by_thread[thread_url]

                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts:
                        start_no = max(1, root.post_no - 5)
                        end_no = root.post_no + 5
                        context_posts = [
                            p
                            for p in all_posts
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    tree_items = build_reply_tree(all_posts, root)

                    anchor_targets: List[ThreadPost] = []
                    if root.anchors:
                        nums = parse_anchors_csv(root.anchors)
                        if nums and all_posts:
                            num_set = set(nums)
                            anchor_targets = [
                                p
                                for p in all_posts
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

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword,
            "thread_filter": thread_filter,
            "tags_input": tags_input,
            "tag_mode": tag_mode,
            "results": thread_results,
            "hit_count": hit_count,
            "highlight": highlight_text,
            "error_message": error_message,
        },
    )


# ====== JSON API（おまけ） ======

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
        query = query.filter(
            or_(
                ThreadPost.thread_url.contains(thread_filter),
                ThreadPost.thread_title.contains(thread_filter),
            )
        )

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


# ====== 取り込み画面（GET / POST） ======

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


# ====== 「このスレだけ再取得」&「このスレだけ削除」 ======

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


# ====== タグ・メモ編集 ======

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


# ====== 外部スレッド検索（タイトル一覧） ======

def parse_posted_at_value(value: str) -> Optional[datetime]:
    """
    スクレイパーで拾った posted_at 文字列を datetime に変換する。
    例: "2025/11/03 06:35"
    """
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def search_threads_external(area_code: str, keyword: str, max_days: Optional[int]) -> List[dict]:
    """
    外部検索サービス(tsr)を叩いて、エリア＋キーワードでスレ一覧を取得。
    さらに必要なら「最終レス日時」で max_days 以内に絞り込む。
    戻り値: {"title", "url", "last_post_at_str"} のリスト。
    """
    if not area_code or not keyword:
        return []

    keyword_for_url = quote_plus(keyword)
    url = f"https://bakusearch.fitlapp.net/tsr?area={area_code}&keyword={keyword_for_url}"

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    threads: List[dict] = []

    # aタグのうち、hrefに bakusai.com を含むものをスレッド候補とする
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "bakusai.com" not in href:
            continue
        title = a.get_text(strip=True)
        full_url = href
        if full_url.startswith("//"):
            full_url = "https:" + full_url
        elif full_url.startswith("/"):
            full_url = "https://bakusai.com" + full_url

        threads.append(
            {
                "title": title,
                "url": full_url,
                "last_post_at_str": None,
            }
        )

    # URLでユニーク化
    unique_by_url: Dict[str, dict] = {}
    for t in threads:
        if t["url"] not in unique_by_url:
            unique_by_url[t["url"]] = t
    threads = list(unique_by_url.values())

    if max_days is None:
        return threads

    now = datetime.now()
    filtered: List[dict] = []

    for t in threads:
        try:
            posts = fetch_posts_from_thread(t["url"])
        except Exception:
            # スクレイプ失敗は無視
            continue

        latest_dt: Optional[datetime] = None
        for p in posts:
            if not getattr(p, "posted_at", None):
                continue
            dt = parse_posted_at_value(p.posted_at)
            if not dt:
                continue
            if latest_dt is None or dt > latest_dt:
                latest_dt = dt

        if not latest_dt:
            continue

        if (now - latest_dt).days <= max_days:
            t["last_post_at_str"] = latest_dt.strftime("%Y-%m-%d %H:%M")
            filtered.append(t)

    # 新しい順にソート
    filtered.sort(key=lambda x: x.get("last_post_at_str") or "", reverse=True)
    return filtered


@app.get("/thread_search", response_class=HTMLResponse)
def thread_search_page(
    request: Request,
    area: str = "7",
    period: str = "2y",
    keyword: str = "",
):
    """
    外部スレッド検索（タイトル一覧）画面。
    GETで area / period / keyword を受け取り、あれば検索する。
    """
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "2y"
    keyword = (keyword or "").strip()

    results: List[dict] = []
    error_message = ""

    # いずれか入力があって keyword があるときだけ検索実行
    if keyword:
        max_days = get_period_days(period)
        try:
            results = search_threads_external(area, keyword, max_days)
        except Exception as e:
            error_message = f"外部検索中にエラーが発生しました: {e}"

    return templates.TemplateResponse(
        "thread_search.html",
        {
            "request": request,
            "area_options": AREA_OPTIONS,
            "period_options": PERIOD_OPTIONS,
            "current_area": area,
            "current_period": period,
            "keyword": keyword,
            "results": results,
            "error_message": error_message,
        },
    )


# ====== 外部スレッド内検索（1スレの中のレス検索） ======

@app.post("/thread_search/posts", response_class=HTMLResponse)
def thread_search_posts(
    request: Request,
    selected_thread: str = Form(""),
    keyword: str = Form(""),
    area: str = Form("7"),
    period: str = Form("2y"),
):
    """
    外部検索で選んだ1スレの中から、キーワードを含むレスだけ表示する。
    DBには保存しないオンメモリ版。
    """
    selected_thread = (selected_thread or "").strip()
    keyword = (keyword or "").strip()
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "2y"

    posts_result: List[dict] = []
    error_message = ""

    if not selected_thread or not keyword:
        error_message = "スレッドまたはキーワードが指定されていません。"
    else:
        try:
            scraped = fetch_posts_from_thread(selected_thread)
            for p in scraped:
                body = p.body or ""
                if keyword in body:
                    posts_result.append(
                        {
                            "post_no": p.post_no,
                            "posted_at": p.posted_at,
                            "body": body,
                        }
                    )
        except Exception as e:
            error_message = f"スレッド内検索中にエラーが発生しました: {e}"

    return templates.TemplateResponse(
        "thread_search_posts.html",
        {
            "request": request,
            "thread_url": selected_thread,
            "keyword": keyword,
            "area": area,
            "period": period,
            "posts": posts_result,
            "error_message": error_message,
            "highlight": highlight_text,
        },
    )
