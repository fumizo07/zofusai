# main.py
import os
import secrets

from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse
from fastapi.templating import Jinja2Templates

from app_lifecycle import register_startup
from routers.internal_search import router as internal_router
from routers.admin import router as admin_router
from routers.threads import router as threads_router
from routers.external_search import router as external_router

# ★ツールチップ用（既存）
from preview_api import preview_api
# ★投稿編集（既存）
from post_edit import post_edit_router

# KB追加（routers import群の近く）
from routers.kb import router as kb_router



# =========================
# BASIC 認証
# =========================
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)


def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
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


# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,
    redoc_url=None,
)

# static / templates
app.mount("/static", StaticFiles(directory="static"), name="static")

# router 登録
app.include_router(preview_api)
app.include_router(post_edit_router)

app.include_router(internal_router)
app.include_router(admin_router)
app.include_router(threads_router)
app.include_router(external_router)

# KB追加（include_router群のどこでもOK）
app.include_router(kb_router)

# startup（DB schema補助・バックフィル）
register_startup(app)


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"



