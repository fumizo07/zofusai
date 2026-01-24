# 003
# main.py
import os
import secrets
import base64

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app_lifecycle import register_startup
from routers.internal_search import router as internal_router
from routers.admin import router as admin_router
from routers.threads import router as threads_router
from routers.external_search import router as external_router

# ★ツールチップ用（既存）
from preview_api import preview_api
# ★投稿編集（既存）
from post_edit import post_edit_router

# KB（既存）
from routers.kb import router as kb_router

# ★追加：KB 料金テンプレAPI
from routers.kb_templates import router as kb_templates_router

# ★追加：KB 日記API
from routers.kb_diary_api import router as kb_diary_api_router


# =========================
# BASIC 認証
# =========================
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)


def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
    """
    FastAPIの依存として使う版（通常ルート向け）
    """
    if not BASIC_ENABLED:
        return
    correct_username = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    correct_password = secrets.compare_digest(credentials.password, BASIC_AUTH_PASS)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


def _basic_ok_from_header(request: Request) -> bool:
    """
    Middleware向け：Authorizationヘッダを自前で検証する
    """
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        return False

    token = auth.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except Exception:
        return False

    username, sep, password = decoded.partition(":")
    if not sep:
        return False

    return (
        secrets.compare_digest(username, BASIC_AUTH_USER)
        and secrets.compare_digest(password, BASIC_AUTH_PASS)
    )


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not BASIC_ENABLED:
            return await call_next(request)

        if _basic_ok_from_header(request):
            return await call_next(request)

        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic"},
        )


# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    docs_url=None,
    redoc_url=None,
)

# ★最重要：StaticFiles含め「全部」にBasicを掛ける
app.add_middleware(BasicAuthMiddleware)

# static
app.mount("/static", StaticFiles(directory="static"), name="static")

# router 登録
app.include_router(preview_api)
app.include_router(post_edit_router)

app.include_router(internal_router)
app.include_router(admin_router)
app.include_router(threads_router)
app.include_router(external_router)

# KB（既存）
app.include_router(kb_router)

# ★追加：KB 料金テンプレAPI
app.include_router(kb_templates_router)

# ★追加：KB 日記API
app.include_router(kb_diary_api_router)

# startup（DB schema補助・バックフィル）
register_startup(app)


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"
