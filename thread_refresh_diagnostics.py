# thread_refresh_diagnostics.py
"""爆サイスレッド全件取得の一時診断と不完全更新防止。"""

from __future__ import annotations

import logging
import time
from typing import Iterable, Optional

import scraper
import services


logger = logging.getLogger("uvicorn.error")
_INSTALLED = False


def _number_range(posts: Iterable[object]) -> tuple[Optional[int], Optional[int], int]:
    numbers: list[int] = []
    count = 0
    for post in posts:
        count += 1
        value = getattr(post, "post_no", None)
        if value is None:
            continue
        try:
            numbers.append(int(value))
        except (TypeError, ValueError):
            continue
    if not numbers:
        return None, None, count
    return min(numbers), max(numbers), count


def install_thread_refresh_diagnostics() -> None:
    """一時診断を有効化し、不完全な全件取得を成功扱いしない。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch_single_page = scraper._fetch_single_page
    original_fetch_posts = scraper.fetch_posts_from_thread

    def traced_fetch_single_page(session, url: str, headers: dict):
        started = time.monotonic()
        try:
            result = original_fetch_single_page(session, url, headers)
        except Exception as exc:
            logger.warning(
                "[THREAD_DIAG][page_error] request_url=%s elapsed=%.2f error=%s",
                url,
                time.monotonic() - started,
                exc,
            )
            raise

        min_no, max_no, count = _number_range(result.posts)
        logger.info(
            "[THREAD_DIAG][page] request_url=%s final_url=%s count=%s min_no=%s max_no=%s elapsed=%.2f",
            url,
            result.final_url,
            count,
            min_no,
            max_no,
            time.monotonic() - started,
        )
        return result

    def traced_fetch_posts(
        url: str,
        max_pages: int = 20,
        stop_at_post_no: Optional[int] = None,
    ):
        started = time.monotonic()
        logger.info(
            "[THREAD_DIAG][start] url=%s max_pages=%s stop_at_post_no=%s",
            url,
            max_pages,
            stop_at_post_no,
        )
        try:
            posts = original_fetch_posts(
                url,
                max_pages=max_pages,
                stop_at_post_no=stop_at_post_no,
            )
        except Exception as exc:
            logger.warning(
                "[THREAD_DIAG][fetch_error] url=%s elapsed=%.2f error=%s",
                url,
                time.monotonic() - started,
                exc,
            )
            raise

        min_no, max_no, count = _number_range(posts)
        reached_oldest = any(getattr(post, "post_no", None) == 1 for post in posts)
        logger.info(
            "[THREAD_DIAG][finish] url=%s count=%s min_no=%s max_no=%s reached_oldest=%s elapsed=%.2f",
            url,
            count,
            min_no,
            max_no,
            reached_oldest,
            time.monotonic() - started,
        )
        return posts

    def guarded_refresh_cached_thread(
        db,
        thread_url: str,
        *,
        full_refresh: bool,
    ) -> None:
        stop_at_post_no = None if full_refresh else services._max_cached_post_no(db, thread_url)
        effective_full_refresh = full_refresh or stop_at_post_no is None
        posts = services.fetch_posts_from_thread(
            thread_url,
            stop_at_post_no=stop_at_post_no,
        )
        post_list = list(posts)
        min_no, max_no, count = _number_range(post_list)
        reached_oldest = any(getattr(post, "post_no", None) == 1 for post in post_list)

        if effective_full_refresh and not reached_oldest:
            logger.warning(
                "[THREAD_DIAG][incomplete_full_refresh] url=%s count=%s min_no=%s max_no=%s action=keep_stale_cache",
                thread_url,
                count,
                min_no,
                max_no,
            )
            raise scraper.ScrapingError(
                "全件取得が#1へ到達しなかったため、キャッシュ更新を完了扱いにしません。"
            )

        services._save_thread_posts_to_cache(
            db,
            thread_url,
            post_list,
            full_refresh=effective_full_refresh,
        )
        logger.info(
            "[THREAD_DIAG][cache_saved] url=%s full=%s count=%s min_no=%s max_no=%s",
            thread_url,
            effective_full_refresh,
            count,
            min_no,
            max_no,
        )

    scraper._fetch_single_page = traced_fetch_single_page
    scraper.fetch_posts_from_thread = traced_fetch_posts
    services.fetch_posts_from_thread = traced_fetch_posts
    services._refresh_cached_thread = guarded_refresh_cached_thread

    _INSTALLED = True
    logger.info("[THREAD_DIAG][installed]")
