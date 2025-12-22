# app_lifecycle.py
from fastapi import FastAPI
from sqlalchemy import text

from db import engine, Base, get_db
from services import cleanup_thread_posts_duplicates, backfill_posted_at_dt, backfill_norm_columns


def register_startup(app: FastAPI) -> None:
    @app.on_event("startup")
    def on_startup():
        Base.metadata.create_all(bind=engine)

        # 既存テーブルに列追加（なければ）
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS posted_at_dt TIMESTAMP"))

            # 揺らぎ検索用の正規化列
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS body_norm TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title_norm TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags_norm TEXT"))

            # 部分ユニークインデックス（post_no NULL は除外）
            try:
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_thread_posts_url_postno "
                        "ON thread_posts(thread_url, post_no) WHERE post_no IS NOT NULL"
                    )
                )
            except Exception:
                pass

            # pg_trgm が使えるなら body_norm に gin_trgm_ops
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS idx_thread_posts_body_norm_trgm "
                        "ON thread_posts USING gin (body_norm gin_trgm_ops)"
                    )
                )
            except Exception:
                pass

        # 重複掃除＆バックフィル（失敗しても起動は継続）
        try:
            db = next(get_db())
            cleanup_thread_posts_duplicates(db)
            backfill_posted_at_dt(db, limit=10000)
            backfill_norm_columns(db, max_total=300000, batch_size=5000)
        except Exception:
            pass
