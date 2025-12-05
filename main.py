import os
import re
from typing import List, Optional, Dict
from collections import defaultdict

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import Column, Integer, Text, create_engine, func, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from markupsafe import Markup, escape

from scraper import fetch_posts_from_thread, ScrapingError

# ====== データベース設定 ======

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が設定されていません。RenderのEnvironmentを確認してください。")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class ThreadPost(Base):
    """
    掲示板のレス1件を表すテーブル。
    """
    __tablename__ = "thread_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)  # 取得元スレURL
    post_no = Column(Integer, nullable=True, index=True)   # レス番号（55など）
    posted_at = Column(Text, nullable=True)                # 投稿日時（文字列）
    body = Column(Text, nullable=False)                    # 本文
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


# ====== テキストハイライト ======

def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文中の検索語を <mark> で囲んで強調表示する。
    HTMLエスケープもここでまとめて行う。
    """
    if text_value is None:
        text_value = ""
    if not keyword:
        return Markup(escape(text_value))

    escaped = escape(text_value)
    pattern = re.compile(re.escape(keyword))

    def repl(match):
        return Markup(f"<mark>{match.group(0)}</mark>")

    highlighted = pattern.sub(lambda m: repl(m), escaped)
    return Markup(highlighted)


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
    # 「どのレスにアンカーしているか」→「その返信のリスト」
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
    ついでに tags, memo カラムが無い場合は追加する。
    """
    Base.metadata.create_all(bind=engine)

    # Postgres に対して DDL を確実にコミットするため engine.begin() を使う
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))


# ====== 検索画面 ======

@app.get("/", response_class=HTMLResponse)
def show_search_page(
    request: Request,
    q: str = "",
    thread_filter: str = "",
    db: Session = Depends(get_db),
):
    """
    ルート画面。
    クエリパラメータ:
      - q: 検索キーワード（必須）
      - thread_filter: スレURLフィルタ（任意。空欄なら全スレ対象）
    各ヒットごとに「前後数レス」と「返信ツリー」を構築して渡す。
    """
    keyword = q.strip()
    thread_filter = thread_filter.strip()
    result_items: List[dict] = []

    if keyword:
        query = db.query(ThreadPost).filter(ThreadPost.body.contains(keyword))
        if thread_filter:
            # 完全一致だとURLのブレが怖いので contains にしておく
            query = query.filter(ThreadPost.thread_url.contains(thread_filter))

        hits: List[ThreadPost] = query.order_by(ThreadPost.id.asc()).all()

        if hits:
            # スレURLごとに全レスキャッシュ
            all_posts_by_thread: Dict[str, List[ThreadPost]] = {}

            for root in hits:
                thread_url = root.thread_url
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

                # 返信ツリー
                tree_items = build_reply_tree(all_posts, root)

                result_items.append(
                    {
                        "root": root,
                        "context": context_posts,
                        "tree": tree_items,
                    }
                )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword,
            "thread_filter": thread_filter,
            "results": result_items,
            "highlight": highlight_text,
        },
    )


# ====== API 検索 ======

@app.get("/api/search")
def api_search(q: str, thread_filter: str = "", db: Session = Depends(get_db)):
    """
    JSONで結果を返すAPI版。
    例: /api/search?q=爆サイ&thread_filter=スレURL
    """
    keyword = q.strip()
    thread_filter = thread_filter.strip()
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
def fetch_thread_get(request: Request):
    """
    取り込み画面の表示専用。
    """
    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": "",
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
    すでに保存されている thread_url については「最大レス番号」以降だけを追加する。
    """
    imported: Optional[int] = None
    error: str = ""

    url = (url or "").strip()

    if url:
        try:
            # 既に保存されている最大レス番号を取得
            last_no = (
                db.query(func.max(ThreadPost.post_no))
                .filter(ThreadPost.thread_url == url)
                .scalar()
            )
            if last_no is None:
                last_no = 0

            scraped_posts = fetch_posts_from_thread(url)
            count = 0
            for sp in scraped_posts:
                # 本文はスクレイパー側で行単位のスペース除去済みだが、念のため strip
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
                    continue

                # 新規行として追加
                db.add(
                    ThreadPost(
                        thread_url=url,
                        post_no=sp.post_no,
                        posted_at=sp.posted_at,
                        body=body,
                        anchors=anchors_str,
                    )
                )
                count += 1

            db.commit()
            imported = count
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
    """
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    if post:
        post.tags = tags.strip() or None
        post.memo = memo.strip() or None
        db.commit()

    # 編集後はトップページに戻る
    return RedirectResponse(url="/", status_code=303)
