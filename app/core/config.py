import os
from datetime import timedelta

# =========================
# DB
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が設定されていません。環境変数 DATABASE_URL を確認してください。")

# =========================
# BASIC 認証
# =========================
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)

# =========================
# 外部検索：スレ全文キャッシュ（DB）
# =========================
THREAD_CACHE_TTL = timedelta(hours=6)   # 好みで調整（例：30分〜24時間）
MAX_CACHED_THREADS = 300                # 好みで調整（例：100〜1000）
