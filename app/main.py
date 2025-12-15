from fastapi import FastAPI, Depends
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.core.security import verify_basic
from app.db.session import Base, engine

from app.routers.search import router as search_router
from app.routers.admin import router as admin_router
from app.routers.threads import router as threads_router
from app.routers.post import router as post_router
from app.routers.thread_search import router as thread_search_router


app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,
    redoc_url=None,
)

# 静的ファイル（CSS 等）
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

    # 既存テーブルに列追加（なければ）
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))

        # ①（高速化）用：正規化済み列（無ければ追加）
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS body_norm TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title_norm TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags_norm TEXT"))


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"


# ルーター登録
app.include_router(search_router)
app.include_router(admin_router)
app.include_router(threads_router)
app.include_router(post_router)
app.include_router(thread_search_router)
