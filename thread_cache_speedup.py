# thread_cache_speedup.py
"""完走済みスレッドの再取得を省略する。"""

from __future__ import annotations

import logging
from datetime import datetime

import services


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False
_COMPLETED_MIN_POST_NO = 1000


def _is_completed_cache(db, thread_url: str) -> bool:
    """#1から#1000以上まで番号の欠落なく保存済みなら完走済みとみなす。"""
    min_no, max_no, distinct_count = (
        db.query(
            services.func.min(services.CachedPost.post_no),
            services.func.max(services.CachedPost.post_no),
            services.func.count(services.func.distinct(services.CachedPost.post_no)),
        )
        .filter(
            services.CachedPost.thread_url == thread_url,
            services.CachedPost.post_no.isnot(None),
        )
        .one()
    )

    if min_no is None or max_no is None:
        return False

    min_no = int(min_no)
    max_no = int(max_no)
    distinct_count = int(distinct_count or 0)
    return (
        min_no == 1
        and max_no >= _COMPLETED_MIN_POST_NO
        and distinct_count == max_no
    )


def _touch_completed_cache(db, thread_url: str) -> None:
    """完走済み判定後は、次回の外部確認期限だけ更新する。"""
    meta = (
        db.query(services.CachedThread)
        .filter(services.CachedThread.thread_url == thread_url)
        .first()
    )
    if meta is None:
        return

    now = datetime.utcnow()
    meta.fetched_at = now
    meta.last_accessed_at = now
    db.commit()


def install_thread_cache_speedup() -> None:
    """完走済みスレッドでは全件補修・増分確認を行わない。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_refresh = services._refresh_cached_thread

    def refresh_cached_thread_fast(
        db,
        thread_url: str,
        *,
        full_refresh: bool,
    ) -> None:
        try:
            if _is_completed_cache(db, thread_url):
                _touch_completed_cache(db, thread_url)
                _LOGGER.info(
                    "[THREAD_CACHE][completed_skip] url=%s full=%s",
                    thread_url,
                    full_refresh,
                )
                return
        except Exception as exc:
            db.rollback()
            _LOGGER.warning(
                "[THREAD_CACHE][completed_check_failed] url=%s error=%s",
                thread_url,
                exc,
            )

        original_refresh(
            db,
            thread_url,
            full_refresh=full_refresh,
        )

    services._refresh_cached_thread = refresh_cached_thread_fast
    _INSTALLED = True
