import os
import re
from typing import List, Optional, Dict
from collections import defaultdict

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
    post_no = Column(Integer, nullable=True, index=True)    # レス番号（例: 55）
    posted_at = Column(Text, nullable=True)                 # 投稿日時（文字列）
    body = Column(Text, nullable=False)                     # 本文
    # 「,55,60,130,」のようにカンマ区切りで保持（後でツリー用に使う）
    anchors = Column(Text, nullable=True)
    # 自分用のタグ・メモ
    tags = Column(Text, nullable=True)
    memo = Column(Text, nullable=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ====== テキスト整形＆ハイライト ======

def _normalize_lines(text_value: str) -> str:
    """
    各行ごとに、先頭の空白・全角スペース・NBSPなどを削る。
    「                        >>251」のような行を「>>251」にするのが目的。
    """
    lines = text_value.splitlines()
    cleaned_lines = []
    for line in lines:
        # 行頭の空白（\s）、全角スペース(U+3000)、NBSP(U+00A0)をまとめて除去
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

    # 表示用に「行頭スペース」を全部削る（DBの中身は変えない）
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
    例: 「〇〇スレッド｜サイト名」 → 「〇〇スレッド」
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

    戻り値は
      [{"post": ThreadPost, "depth": 0}, {"post": ThreadPost, "depth": 1}, ...]
    というリスト（表示しやすいようにフラット＋深さ情報）。
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
        # ルート自身は別枠表示なのでここでは追加しない
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


# ====== FastAPI アプリ本体 ======

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def on_startup():
    """
    アプリ起動時にテーブルを自動作成。
    ついでに追加カラムが無い場合は追加する。
    """
    Base.metadata.create_all(bind=engine)

    # Postgres に対して DDL を確実にコミットするため engine.begin() を使う
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

    # 既に保存されている最大レス番号を取得
    last_no = (
        db.query(func.max(ThreadPost.post_no))
        .filter(ThreadPost.thread_url == url)
        .scalar()
    )
    if last_no is None:
        last_no = 0

    # スレタイトル（ページタイトルなど）を取得
    thread_title = get_thread_title(url)
    if thread_title:
        thread_title = simplify_thread_title(thread_title)

    scraped_posts = fetch_posts_from_thread(url)
    count = 0

    for sp in scraped_posts:
        body = (sp.body or "").strip()
        if not body:
            continue

        # すでに取得済みのレス番号まではスキップ
        if sp.post_no is not None and sp.post_no <= last_no:
            continue

        # アンカーリストを ",55,60," のような文字列に変換
        if sp.anchors:
            anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
        else:
            anchors_str = None

        # 念のため重複チェック（URL + post_no）
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
            # 既存行があれば、足りない情報だけ更新してスキップ
            if not existing.posted_at and sp.posted_at:
                existing.posted_at = sp.posted_at
            if not existing.anchors and anchors_str:
                existing.anchors = anchors_str
            if thread_title and not existing.thread_title:
                existing.thread_title = thread_title
            continue

        # 新規行として追加
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


# ====== 検索画面 ======

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
    トップ画面。
    クエリパラメータ:
      - q: 検索キーワード
      - thread_filter: スレURLフィルタ（任意）
      - tags: タグフィルタ（カンマ区切り）
      - tag_mode: "or" または "and"
    検索結果はスレ単位でまとめて返す。
    """
    # None が来ても必ず空文字にしてから strip する
    keyword = (q or "").strip()
    thread_filter = (thread_filter or "").strip()
    tags_input = (tags or "").strip()
    tag_mode = (tag_mode or "or").lower()

    thread_results: List[dict] = []
    hit_count = 0
    error_message: str = ""

    try:
        # 何かしら条件が入っているときだけ検索
        if keyword or thread_filter or tags_input:
            query = db.query(ThreadPost)

            if keyword:
                query = query.filter(ThreadPost.body.contains(keyword))
            if thread_filter:
                query = query.filter(ThreadPost.thread_url.contains(thread_filter))

            # タグフィルタ
            tags_list: List[str] = []
            if tags_input:
                tags_list = [t.strip() for t in tags_input.split(",") if t.strip()]

            if tags_list:
                if tag_mode == "and":
                    # AND 条件: すべてのタグを含む
                    for t in tags_list:
                        query = query.filter(ThreadPost.tags.contains(t))
                else:
                    # OR 条件: いずれかのタグを含む
                    or_conditions = [ThreadPost.tags.contains(t) for t in tags_list]
                    query = query.filter(or_(*or_conditions))

            hits: List[ThreadPost] = (
                query.order_by(ThreadPost.thread_url.asc(), ThreadPost.post_no.asc()).all()
            )
            hit_count = len(hits)

            if hits:
                # スレURLごとに全レスキャッシュ
                all_posts_by_thread: Dict[str, List[ThreadPost]] = {}
                # スレURL → スレ結果ブロック
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
                            "entries": [],   # ← ここを items ではなく entries に
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

                    # 前後コンテキスト（±2レス）
                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts:
                        start_no = max(1, root.post_no - 2)
                        end_no = root.post_no + 2
                        context_posts = [
                            p
                            for p in all_posts
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    # 返信ツリー（このレスに向かってアンカーしている側）
                    tree_items = build_reply_tree(all_posts, root)

                    # アンカー先（このレスから >>X している側）
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

                    block["entries"].append(  # ← ここも entries
                        {
                            "root": root,
                            "context": context_posts,
                            "tree": tree_items,
                            "anchor_targets": anchor_targets,
                        }
                    )

    except Exception as e:
        # ここで 500 にはせず、画面上にエラーメッセージを出すだけにする
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


# ====== API 検索（シンプル版） ======

@app.get("/api/search")
def api_search(
    q: str,
    thread_filter: str = "",
    db: Session = Depends(get_db),
):
    """
    JSONで結果を返すAPI版（簡易）。
    例: /api/search?q=テスト&thread_filter=スレURL
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


# ====== 取り込み画面（GET） ======

@app.get("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_get(request: Request, url: str = ""):
    """
    スレ取り込み画面の表示専用。
    クエリパラメータ url があれば初期値として表示。
    """
    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": url or "",
            "imported": None,
            "error": "",
        },
    )


# ====== 取り込み処理（POST） ======

@app.post("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_post(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    フォームから送信された URL を元にスクレイピングして DB に保存。
    """
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


# ====== 検索画面から「このスレだけ再取得」 ======

@app.post("/admin/refetch")
def refetch_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    検索結果画面から「このスレだけ再取得」するためのエンドポイント。
    終了後は元の画面（Referer）に戻る。
    """
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)


# ====== タグ・メモ編集用エンドポイント ======

@app.get("/post/{post_id}/edit", response_class=HTMLResponse)
def edit_post_get(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
):
    """
    指定IDのレスに対してタグ・メモを編集する画面（GET）。
    """
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
    """
    タグ・メモの更新処理（POST）。
    編集後は元の画面（Referer）があればそこに戻る。
    """
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    if post:
        post.tags = (tags or "").strip() or None
        post.memo = (memo or "").strip() or None
        db.commit()

    back_url = request.headers.get("referer") or "/"
    return RedirectResponse(url=back_url, status_code=303)
