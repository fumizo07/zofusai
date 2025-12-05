import os
import re
from typing import List, Optional
from collections import defaultdict 

from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ====== FastAPI アプリ本体 ======

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def on_startup():
    """
    アプリ起動時にテーブルを自動作成。
    ※サンプルデータ投入はやめて、実際の取り込みだけにします。
    """
    Base.metadata.create_all(bind=engine)


@app.get("/", response_class=HTMLResponse)
def show_search_page(request: Request, q: str = "", db: Session = Depends(get_db)):
    """
    ルート画面。
    クエリパラメータ q があれば thread_posts.body を LIKE 検索。
    """
    keyword = q.strip()
    results: List[ThreadPost] = []

    if keyword:
        results = (
            db.query(ThreadPost)
            .filter(ThreadPost.body.contains(keyword))
            .order_by(ThreadPost.id.asc())
            .all()
        )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword,
            "results": results,
        },
    )


@app.get("/api/search")
def api_search(q: str, db: Session = Depends(get_db)):
    """
    JSONで結果を返すAPI版。
    例: /api/search?q=テスト
    """
    keyword = q.strip()
    if not keyword:
        return []

    posts = (
        db.query(ThreadPost)
        .filter(ThreadPost.body.contains(keyword))
        .order_by(ThreadPost.id.asc())
        .all()
    )
    return [
        {
            "id": p.id,
            "thread_url": p.thread_url,
            "post_no": p.post_no,
            "posted_at": p.posted_at,
            "body": p.body,
            "anchors": p.anchors,
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
    """
    imported: Optional[int] = None
    error: str = ""

    url = (url or "").strip()

    if url:
        try:
            scraped_posts = fetch_posts_from_thread(url)
            count = 0
            for sp in scraped_posts:
                body = (sp.body or "").strip()
                if not body:
                    continue

                # アンカーリストを ",55,60," のような文字列に変換
                if sp.anchors:
                    anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
                else:
                    anchors_str = None

                # ===== 重複チェック =====
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
                    # 念のため post_no が無い場合は本文でざっくり判定
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

                # ===== 新規行として追加 =====
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
    # 重複排除してソート
    return sorted(set(nums))


def build_reply_tree(all_posts: List[ThreadPost], root: ThreadPost) -> List[dict]:
    """
    1スレ内の全レスから、
    「root.post_no にアンカーを飛ばしているレス」を起点に木構造を作る。

    戻り値は
      [{"post": ThreadPost, "depth": 0}, {"post": ThreadPost, "depth": 1}, ...]
    というリスト（表示しやすいようにフラット＋深さ情報）。
    """
    # post_no -> ThreadPost
    by_no: dict[int, ThreadPost] = {}
    for p in all_posts:
        if p.post_no is not None:
            by_no[p.post_no] = p

    # 「どのレスにアンカーしているか」→「その返信のリスト」
    # target_no -> [ThreadPost, ...]
    replies = defaultdict(list)
    for p in all_posts:
        for a in parse_anchors_csv(p.anchors):
            replies[a].append(p)

    result: List[dict] = []
    visited_ids: set[int] = set()

    def dfs(post: ThreadPost, depth: int) -> None:
        if post.id in visited_ids:
            return
        visited_ids.add(post.id)
        if post.id != root.id:  # ルートは別枠で表示するので除外
            result.append({"post": post, "depth": depth})
        # 自分のレス番号に対する返信をたどる
        if post.post_no is None:
            return
        for child in replies.get(post.post_no, []):
            dfs(child, depth + 1)

    # ルートに対する返信を起点に DFS
    if root.post_no is not None:
        for child in replies.get(root.post_no, []):
            dfs(child, 0)

    return result


# ====== レスツリー表示用エンドポイント ======

@app.get("/post/{post_id}", response_class=HTMLResponse)
def show_post_tree(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
):
    """
    指定IDのレスをルートとして、
    同じスレ内でそのレスにアンカーしている返信ツリーを表示する。
    """
    root_post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    if not root_post:
        return templates.TemplateResponse(
            "post.html",
            {
                "request": request,
                "root_post": None,
                "tree_items": [],
            },
        )

    # 同じスレURLの全レスを取得（レス番号順）
    all_posts = (
        db.query(ThreadPost)
        .filter(ThreadPost.thread_url == root_post.thread_url)
        .order_by(ThreadPost.post_no.asc())
        .all()
    )

    tree_items = build_reply_tree(all_posts, root_post)

    return templates.TemplateResponse(
        "post.html",
        {
            "request": request,
            "root_post": root_post,
            "tree_items": tree_items,
        },
    )
from collections import defaultdict

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
    # post_no -> ThreadPost
    by_no: dict[int, ThreadPost] = {}
    for p in all_posts:
        if p.post_no is not None:
            by_no[p.post_no] = p

    # 「どのレスにアンカーしているか」→「その返信のリスト」
    replies = defaultdict(list)
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
