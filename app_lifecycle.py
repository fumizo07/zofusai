# 005
# app_lifecycle.py
from fastapi import FastAPI
from sqlalchemy import text

from db import engine, Base, get_db
from services import cleanup_thread_posts_duplicates, backfill_posted_at_dt, backfill_norm_columns


def register_startup(app: FastAPI) -> None:
    @app.on_event("startup")
    def on_startup():
        Base.metadata.create_all(bind=engine)

        with engine.begin() as conn:
            # =========================
            # Thread 系
            # =========================
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS posted_at_dt TIMESTAMP"))

            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS body_norm TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title_norm TEXT"))
            conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags_norm TEXT"))

            try:
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_thread_posts_url_postno "
                        "ON thread_posts(thread_url, post_no) WHERE post_no IS NOT NULL"
                    )
                )
            except Exception:
                pass

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

            # =========================
            # KB 系（不足カラムを後付け）
            # =========================

            # kb_regions / kb_stores（将来用）
            try:
                conn.execute(text("ALTER TABLE kb_regions ADD COLUMN IF NOT EXISTS name_norm TEXT"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_stores ADD COLUMN IF NOT EXISTS name_norm TEXT"))
            except Exception:
                pass

            # kb_persons
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS age INTEGER"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS cup TEXT"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS services TEXT"))
            except Exception:
                pass

            # 揺らぎ検索用
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS name_norm TEXT"))
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS services_norm TEXT"))
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS tags_norm TEXT"))
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS memo_norm TEXT"))
            except Exception:
                pass

            # 重要：検索用まとめ列（今回500原因になったやつ）
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS search_norm TEXT"))
            except Exception:
                pass

            # kb_visits（利用ログ）
            # 運用は start_min / end_min（分）で統一する
            try:
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS start_min INTEGER"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS end_min INTEGER"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS duration_min INTEGER"))
            except Exception:
                pass
            try:
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS search_norm TEXT"))
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
