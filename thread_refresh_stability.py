# thread_refresh_stability.py
"""爆サイのページ固定応答と一時的なブラウザ取得失敗を吸収する。"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Optional

import scraper
import services
import thread_refresh_browser as browser_refresh
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False
_BROWSER_NAVIGATION_ATTEMPTS = 3
_FULL_REFRESH_MODE: ContextVar[bool] = ContextVar(
    "thread_full_refresh_mode",
    default=False,
)


def _numbered_signature(posts) -> tuple[int, ...]:
    return tuple(
        sorted(
            int(post.post_no)
            for post in posts
            if getattr(post, "post_no", None) is not None
        )
    )


def _fetch_page_with_stuck_detection(
    session,
    url: str,
    headers: dict,
    *,
    referer: Optional[str] = None,
):
    """全件取得時に同じレス範囲が繰り返されたらブラウザへ切り替える。"""
    if _FULL_REFRESH_MODE.get() and getattr(
        session,
        "_zofusai_force_browser_fallback",
        False,
    ):
        raise scraper.ScrapingError(
            "requestsのページ送りが同じレス範囲に固定されたため、"
            "ブラウザ取得へ切り替えます。"
        )

    posts, final_url, links = _ORIGINAL_FETCH_PAGE(
        session,
        url,
        headers,
        referer=referer,
    )

    if not _FULL_REFRESH_MODE.get():
        return posts, final_url, links

    signature = _numbered_signature(posts)
    if not signature:
        return posts, final_url, links

    page_no = refresh_fix._page_number(url)
    seen_signatures = getattr(session, "_zofusai_page_signatures", None)
    if seen_signatures is None:
        seen_signatures = set()
        setattr(session, "_zofusai_page_signatures", seen_signatures)

    if page_no < 10**9:
        explicit_count = int(
            getattr(session, "_zofusai_explicit_page_count", 0)
        ) + 1
        setattr(session, "_zofusai_explicit_page_count", explicit_count)

        if explicit_count >= 2 and signature in seen_signatures:
            setattr(session, "_zofusai_force_browser_fallback", True)
            min_no = min(signature)
            max_no = max(signature)
            _LOGGER.warning(
                "[THREAD_REFRESH][requests_pager_stuck] url=%s page=%s "
                "range=%s-%s action=browser_fallback",
                url,
                page_no,
                min_no,
                max_no,
            )

    seen_signatures.add(signature)
    return posts, final_url, links


def _crawl_with_stuck_detection(
    url: str,
    max_pages: int = 20,
    stop_at_post_no: Optional[int] = None,
):
    token = _FULL_REFRESH_MODE.set(stop_at_post_no is None)
    try:
        return _ORIGINAL_CRAWL(
            url,
            max_pages=max_pages,
            stop_at_post_no=stop_at_post_no,
        )
    finally:
        _FULL_REFRESH_MODE.reset(token)


def _navigate_with_recovery(page, target_url: str, source_url: Optional[str]):
    """投稿0件の一時応答時に、待機・fetch・再遷移で回復を試す。"""
    last_error: Optional[Exception] = None

    for attempt in range(1, _BROWSER_NAVIGATION_ATTEMPTS + 1):
        try:
            return _ORIGINAL_NAVIGATE(page, target_url, source_url)
        except Exception as exc:
            last_error = exc
            _LOGGER.warning(
                "[THREAD_BROWSER][navigation_retry] target_url=%s attempt=%s "
                "reason=%s",
                target_url,
                attempt,
                browser_refresh._error_text(exc),
            )

        try:
            page.wait_for_timeout(600 * attempt)
        except Exception:
            pass

        try:
            fetched_results = browser_refresh._fetch_html_batch(
                page,
                [(target_url, source_url)],
            )
            if fetched_results:
                result = _ORIGINAL_PARSE_FETCHED_HTML(
                    fetched_results[0],
                    target_url,
                )
                _LOGGER.info(
                    "[THREAD_BROWSER][navigation_fetch_recovery] "
                    "target_url=%s attempt=%s",
                    target_url,
                    attempt,
                )
                return result
        except Exception as fetch_exc:
            last_error = fetch_exc
            _LOGGER.info(
                "[THREAD_BROWSER][navigation_fetch_retry] target_url=%s "
                "attempt=%s reason=%s",
                target_url,
                attempt,
                browser_refresh._error_text(fetch_exc),
            )

    if isinstance(last_error, scraper.ScrapingError):
        raise last_error
    raise scraper.ScrapingError(
        f"ブラウザ初回取得の再試行に失敗しました: {last_error}"
    )


def install_thread_refresh_stability() -> None:
    """requests固定応答の検出とPlaywright初回取得の再試行を有効化する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    refresh_fix._fetch_page = _fetch_page_with_stuck_detection
    refresh_fix._crawl_thread_pages = _crawl_with_stuck_detection
    scraper.fetch_posts_from_thread = _crawl_with_stuck_detection
    services.fetch_posts_from_thread = _crawl_with_stuck_detection
    browser_refresh._navigate_and_parse = _navigate_with_recovery
    _INSTALLED = True


_ORIGINAL_FETCH_PAGE = refresh_fix._fetch_page
_ORIGINAL_CRAWL = refresh_fix._crawl_thread_pages
_ORIGINAL_NAVIGATE = browser_refresh._navigate_and_parse
_ORIGINAL_PARSE_FETCHED_HTML = browser_refresh._parse_fetched_html
