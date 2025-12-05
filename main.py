import os
from typing import List

from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ====== データベース設定 ======

# Render の Environment で設定した DATABASE_URL を取得
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # ローカル開発用に SQLite を使いたい場合はここに
    # DATABASE_URL = "sqlite:///./test.db"
    # のように書いてもよいですが、
    # Render 上では必ず環境変数をセットしてください。
    raise RuntimeError("DATABASE_URL が設定されていません。RenderのEnvironmentを確認してください。")

# SQLAlchemy エンジンとセッション
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# モデルのベースクラス
Base = declarative_base()


class Post(Base):
    """
    投稿テーブル（posts）
    今は text だけですが、あとからスレURLやレス番号などを追加していきます。
    """
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text, nullable=False)


def get_db():
    """
    リクエストごとに DB セッションを開いて閉じるための依存関係。
    FastAPI の Depends で使います。
    """
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
    # テーブル作成（既にあれば何もしない）
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        count = db.query(Post).count()
        if count == 0:
            # 初回だけテストデータを投入
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
        # 非常に単純な部分一致検索
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
