# 003
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
            # =========================
            # Thread 系
            # =========================
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

            # =========================
            # KB 系（不足カラムを後付け）
            # =========================

            # kb_regions
            try:
                conn.execute(text("ALTER TABLE kb_regions ADD COLUMN IF NOT EXISTS name_norm TEXT"))
            except Exception:
                pass

            # kb_stores
            try:
                conn.execute(text("ALTER TABLE kb_stores ADD COLUMN IF NOT EXISTS name_norm TEXT"))
            except Exception:
                pass

            # kb_persons（今回落ちた age ほか）
            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS age INTEGER"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE kb_persons ADD COLUMN IF NOT EXISTS cup TEXT"))
            except Exception:
                pass

            # ※あなたのログ上は services を参照している（service ではなく services）
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

            # kb_visits（利用時間/表示崩れ防止のため、将来参照されても落ちないように）
            try:
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS start_min INTEGER"))
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS end_min INTEGER"))
                conn.execute(text("ALTER TABLE kb_visits ADD COLUMN IF NOT EXISTS duration_min INTEGER"))
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
