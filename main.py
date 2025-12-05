import os
from typing import List, Optional

from fastapi import FastAPI, Request, Depends
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


class Post(Base):
    """
    投稿テーブル（posts）
    今は text だけですが、あとでスレURLやレス番号などを追加していきます。
    """
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text, nullable=False)


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
    アプリ起動時にテーブルを自動作成し、
    posts テーブルが空ならテスト投稿を3件だけ入れておきます。
    """
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        count = db.query(Post).count()
        if count == 0:
            samples = [
                Post(text="これはDBに保存されたテスト投稿1です。キーワード：テスト"),
                Post(text="これはDBに保存されたテスト投稿2です。キーワード：検索ツール"),
                Post(text="これはDBに保存されたテスト投稿3です。アンカー >>1 の例。"),
            ]
            db.add_all(samples)
            db.commit()
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
def show_search_page(request: Request, q: str = "", db: Session = Depends(get_db)):
    """
    ルート画面。
    クエリパラメータ q があれば posts テーブルを LIKE 検索して結果を表示します。
    例: /?q=テスト
    """
    keyword = q.strip()
    results: List[Post] = []

    if keyword:
        results = db.query(Post).filter(Post.text.contains(keyword)).all()

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

    posts = db.query(Post).filter(Post.text.contains(keyword)).all()
    return [{"id": p.id, "text": p.text} for p in posts]


@app.get("/admin/fetch", response_class=HTMLResponse)
def fetch_thread(request: Request, url: str = "", db: Session = Depends(get_db)):
    """
    簡易取り込み画面。
    /admin/fetch?url=... にアクセスすると、そのURLから投稿を取得してDBに保存します。
    フォーム（fetch.html）は method="get" なので、URLを入れて送信すると同じ処理です。
    """
    imported: Optional[int] = None
    error: str = ""

    if url:
        try:
            texts = fetch_posts_from_thread(url)
            for text in texts:
                text = text.strip()
                if not text:
                    continue
                db.add(Post(text=text))
            db.commit()
            imported = len(texts)
        except ScrapingError as e:
            db.rollback()
            error = str(e)
        except Exception as e:
            db.rollback()
            error = f"想定外のエラーが発生しました: {e}"

    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": url,
            "imported": imported,
            "error": error,
        },
    )
