import os
import re
import unicodedata
from typing import List, Optional, Dict
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import Column, Integer, Text, create_engine, func, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from markupsafe import Markup, escape

from scraper import fetch_posts_from_thread, ScrapingError, get_thread_title


# =========================
# DB セットアップ
# =========================

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
    thread_url = Column(Text, nullable=False, index=True)
    thread_title = Column(Text, nullable=True)
    post_no = Column(Integer, nullable=True, index=True)
    posted_at = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    anchors = Column(Text, nullable=True)  # ",55,60,130," のような形式
    tags = Column(Text, nullable=True)     # 自分用タグ（カンマ区切り想定）
    memo = Column(Text, nullable=True)     # 自分用メモ


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================
# エリア・期間設定（全国対応）
# =========================

AREA_OPTIONS = [
    {"code": "",   "label": "地域を選択"},
    {"code": "1",  "label": "北海道版"},
    {"code": "14", "label": "北東北版（青森・岩手・秋田）"},
    {"code": "2",  "label": "南東北版（宮城・山形・福島）"},
    {"code": "15", "label": "北関東版（茨城・栃木・群馬）"},
    {"code": "3",  "label": "南関東・東京周辺版"},
    {"code": "4",  "label": "甲信越版（新潟・長野・山梨）"},
    {"code": "6",  "label": "北陸版（富山・石川・福井）"},
    {"code": "5",  "label": "東海版（愛知・岐阜・静岡・三重）"},
    {"code": "18", "label": "関西版（滋賀・京都・兵庫・奈良・和歌山）"},
    {"code": "7",  "label": "大阪版（大阪単独）"},
    {"code": "8",  "label": "山陽版（岡山・広島・山口）"},
    {"code": "12", "label": "山陰版（鳥取・島根）"},
    {"code": "9",  "label": "四国版（徳島・香川・愛媛・高知）"},
    {"code": "10", "label": "北部九州版（福岡・佐賀・長崎・大分）"},
    {"code": "16", "label": "南部九州版（熊本・宮崎・鹿児島）"},
    {"code": "11", "label": "沖縄版"},
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
    return PERIOD_ID_TO_DAYS.get(period_id)


# =========================
# テキスト正規化・ハイライト
# =========================

def _normalize_lines(text_value: str) -> str:
    """
    本文用：各行の先頭空白（半角/全角/NBSP）を削除。
    先頭側の完全な空行の連続は削除して、余計な上部スペースも潰す。
    """
    lines = text_value.splitlines()
    cleaned: List[str] = []
    leading = True
    for line in lines:
        if leading and line.strip() == "":
            continue
        line = re.sub(r'^[\s\u3000\xa0]+', '', line)
        cleaned.append(line)
        leading = False
    return "\n".join(cleaned)


def to_hiragana(s: str) -> str:
    """
    カタカナ → ひらがな変換。
    """
    result = []
    for ch in s:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:  # カタカナ領域
            result.append(chr(code - 0x60))
        else:
            result.append(ch)
    return "".join(result)


def normalize_for_search(s: Optional[str]) -> str:
    """
    検索用正規化：
    - None → ""
    - NFKC 正規化
    - カタカナ → ひらがな
    - 英字は小文字化
    """
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = to_hiragana(s)
    s = s.lower()
    return s


def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文中の検索語を <mark> で囲んで強調表示。
    - 文頭の変なスペースは除去
    - 英字については大文字小文字を無視してハイライト
    （ひらがな↔カタカナのハイライトまではやらず、検索だけ対応）
    """
    if text_value is None:
        text_value = ""
    text_value = _normalize_lines(text_value)

    if not keyword:
        return Markup(escape(text_value))

    escaped = escape(text_value)
    try:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    except re.error:
        return Markup(escaped)

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


# =========================
# アンカー／ツリー関連
# =========================

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
    1スレ内の全レスから root への返信ツリーを作る（DB内検索用）。
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
    """
    "2025/11/03 06:35" のような日時を datetime に変換。
    （今は外部検索の内部処理でのみ利用）
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


# =========================
# FastAPI 本体
# =========================

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def on_startup():
    """
    アプリ起動時のテーブル作成＋不足カラムの追加。
    """
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))


# =========================
# スレ取り込み共通処理
# =========================

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


# =========================
# 内部検索（トップ /）
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
    """
    Personal Search メイン画面。
    - ひらがな／カタカナ、大文字小文字を区別しない検索
    - 本文・スレURL/タイトル・タグに同じ正規化を適用
    """
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

    try:
        # 何かしら条件が入っている場合のみ検索
        if keyword_raw or thread_filter_raw or tags_input_raw:
            # いったん全件取得して Python 側で判定（個人用ツールなので許容）
            all_posts: List[ThreadPost] = (
                db.query(ThreadPost)
                .order_by(ThreadPost.thread_url.asc(), ThreadPost.post_no.asc())
                .all()
            )

            # スレURLごとにまとめておく（コンテキスト・ツリー用）
            posts_by_thread: Dict[str, List[ThreadPost]] = defaultdict(list)
            for p in all_posts:
                posts_by_thread[p.thread_url].append(p)

            hits: List[ThreadPost] = []

            for p in all_posts:
                body_norm = normalize_for_search(p.body or "")

                # 本文キーワード
                if keyword_norm:
                    if keyword_norm not in body_norm:
                        continue

                # スレURL / タイトルフィルタ
                if thread_filter_norm:
                    url_norm = normalize_for_search(p.thread_url or "")
                    title_norm = normalize_for_search(p.thread_title or "")
                    if thread_filter_norm not in url_norm and thread_filter_norm not in title_norm:
                        continue

                # タグフィルタ
                if tags_norm_list:
                    post_tags_norm = normalize_for_search(p.tags or "")
                    if tag_mode == "and":
                        # 全て含まれている必要あり
                        ok = all(t in post_tags_norm for t in tags_norm_list)
                    else:
                        # どれか一つでも含まれていればOK
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
                            "entries": [],
                        }
                        thread_map[thread_url] = block
                        thread_results.append(block)

                    all_posts_thread = posts_by_thread.get(thread_url, [])

                    # コンテキスト：前後5レス
                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts_thread:
                        start_no = max(1, root.post_no - 5)
                        end_no = root.post_no + 5
                        context_posts = [
                            p
                            for p in all_posts_thread
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    # 返信ツリー
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
        },
    )


# =========================
# JSON API（おまけ）
# =========================

@app.get("/api/search")
def api_search(
    q: str,
    thread_filter: str = "",
    db: Session = Depends(get_db),
):
    """
    API は簡易版として DB の contains ベースのまま（細かい正規化はなし）。
    """
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
# 取り込み画面（管理用）
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


# 「このスレだけ再取得」ボタン用
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


# 「このスレだけ削除」ボタン用
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
# タグ・メモ編集
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
# 外部スレッド検索（タイトル一覧）
# =========================

def search_threads_external(area_code: str, keyword: str, max_days: Optional[int]) -> List[dict]:
    """
    爆サイ本体の「スレッド検索結果」ページから
    ・タイトル
    ・URL
    ・最新レス日時
    を取り出す。
    タイトルについても normalize_for_search で比較する。
    """
    keyword = (keyword or "").strip()
    if not area_code or not keyword:
        return []

    url = f"https://bakusai.com/sch_thr_thread/acode={area_code}/word={quote_plus(keyword)}"

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
            # 古すぎるものはスキップ
            continue

        parent = s.parent
        link = None
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

        # タイトルも正規化して比較（ひらがな/カタカナ・大小文字を区別しない）
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
    area: str = "7",      # デフォルト：大阪版
    period: str = "3m",   # デフォルト：3ヶ月以内
    keyword: str = "",
):
    """
    外部スレッド検索（タイトル一覧）画面。
    keyword は「タイトルに含めたい語句」として扱う。
    """
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"
    keyword = (keyword or "").strip()

    results: List[dict] = []
    error_message = ""

    if keyword and area:
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


# =========================
# 外部スレッドの前後スレ解析
# =========================

def find_prev_next_thread_urls(thread_url: str) -> tuple[Optional[str], Optional[str]]:
    """
    爆サイのスレページから「前スレ」「次スレ」らしきリンクをざっくり探す。
    （テキストに「前」「スレ」／「次」「スレ」が含まれている a タグ）
    """
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

    prev_href: Optional[str] = None
    next_href: Optional[str] = None

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/thr_res/" not in href:
            continue
        text = (a.get_text() or "").strip()
        if not text:
            continue

        if ("前" in text and "スレ" in text) and prev_href is None:
            prev_href = href
        if ("次" in text and "スレ" in text) and next_href is None:
            next_href = href

    def normalize_href(h: Optional[str]) -> Optional[str]:
        if not h:
            return None
        if h.startswith("//"):
            return "https:" + h
        if h.startswith("/"):
            return "https://bakusai.com" + h
        return h

    return normalize_href(prev_href), normalize_href(next_href)


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
):
    """
    外部検索で選んだ1スレの中から、
    「post_keyword」を含むレスだけ表示する。
    - 本文検索は normalize_for_search でひらがな/カタカナ、大文字小文字を区別しない
    - index.html と似た UI（本文／アンカー先／ツリー／前後コンテキスト）
    - 前スレ・次スレの URL を取れる場合はボタンを表示
    """
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

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    else:
        try:
            # タイトル取得（画面表示用）
            try:
                t = get_thread_title(selected_thread)
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

            # 前スレ／次スレ
            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread)

            # 全レス取得（オンメモリ）
            all_posts = fetch_posts_from_thread(selected_thread)

            # レス番号 → Post
            posts_by_no: Dict[int, object] = {}
            for p in all_posts:
                if p.post_no is not None and p.post_no not in posts_by_no:
                    posts_by_no[p.post_no] = p

            # 返信ツリー用インデックス
            replies: Dict[int, List[object]] = defaultdict(list)
            for p in all_posts:
                if not getattr(p, "anchors", None):
                    continue
                # scraper 側では anchors はたぶん List[int] なのでそのまま使う
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

            # 正規化キーワード
            post_keyword_norm = normalize_for_search(post_keyword)

            # ヒットしたレスだけ拾う
            for root in all_posts:
                body = root.body or ""
                body_norm = normalize_for_search(body)
                if post_keyword_norm not in body_norm:
                    continue

                # コンテキスト（前後5レス）
                context_posts: List[object] = []
                if root.post_no is not None:
                    start_no = max(1, root.post_no - 5)
                    end_no = root.post_no + 5
                    for p in all_posts:
                        if p.post_no is None:
                            continue
                        if start_no <= p.post_no <= end_no:
                            context_posts.append(p)

                # ツリー
                tree_items = build_reply_tree_external(root)

                # アンカー先
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
        },
    )
