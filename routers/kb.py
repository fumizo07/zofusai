# 027
# routers/kb.py
from fastapi import APIRouter

from .kb_pages import router as pages_router
from .kb_diary_api import router as diary_router
from .kb_templates import router as templates_router

router = APIRouter()

# pages (/kb, CRUD, search, import/export)
router.include_router(pages_router)

# diary NEW check APIs
router.include_router(diary_router)

# price templates APIs
router.include_router(templates_router)
