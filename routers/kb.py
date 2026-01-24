# 027
# routers/kb.py
from __future__ import annotations

from fastapi import APIRouter

from .kb_parts.diary_api import router as diary_router
from .kb_parts.price_templates_api import router as price_templates_router
from .kb_parts.pages import router as pages_router
from .kb_parts.backup import router as backup_router


router = APIRouter()

# 表示ページ / CRUD
router.include_router(pages_router)

# API
router.include_router(diary_router)
router.include_router(price_templates_router)

# バックアップ系 / panic
router.include_router(backup_router)
