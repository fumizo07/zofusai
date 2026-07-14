# thread_refresh_browser.py
"""requestsで全件取得できない場合にPlaywrightで再取得する。"""

from __future__ import annotations

import logging
from collections import deque
from typing import Optional

import scraper
import services
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False


def _has_oldest(posts) -> bool:
    return any(getattr(post, "post_no", None) == 1 for post in posts)


def _merge_posts(target, posts, seen_post_nos, seen_unknown) -> None:
    for post in posts:
        post_no = getattr(post, "post_no", None)
        if post_no is not None:
            post_no = int(post_no)
            if post_no in seen_post_nos:
                continue
            seen_post_nos.add(post_no)
        else:
            unknown_key = (
                getattr(post, "posted_at", None),
                getattr(post, "body", "") or "",
            )
            if unknown_key in seen_unknown:
                continue
            seen_unknown.add(unknown_key)
        target.append(post)


def _error_text(exc: Exception) -> str:
    """ログ用に例外の種類と内容を短く整形する。"""
    message = " ".join(str(exc).split())
    if len(message) > 400:
        message = message[:397] + "..."
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _configure_page(page) -> None:
    page.set_default_navigation_timeout(25_000)
    page.set_default_timeout(25_000)
    try:
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "media", "font")
            else route.continue_(),
        )
    except Exception:
        pass


def _read_page(context, target_url: str, source_url: Optional[str]):
    """
    同じブラウザコンテキスト内の新しいページでURLを直接開く。

    以前の実装はリンクをクリックした直後に別のgotoを重ねる可能性があり、
    Playwrightのナビゲーション同士が競合していた。
    """
    page = context.new_page()
    _configure_page(page)
    response = None
    navigation_error: Optional[Exception] = None

    try:
        try:
            goto_options = {"wait_until": "domcontentloaded"}
            if source_url:
                goto_options["referer"] = source_url
            response = page.goto(target_url, **goto_options)
        except Exception as exc:
            navigation_error = exc
            try:
                page.wait_for_timeout(800)
            except Exception:
                pass

        final_url = page.url or target_url
        html = page.content() or ""
        soup = scraper.BeautifulSoup(html, "html.parser")
        posts = scraper._parse_posts_from_soup(soup)

        if not posts:
            status = getattr(response, "status", None) if response is not None else None
            if navigation_error is not None:
                raise scraper.ScrapingError(
                    "ページ遷移後も投稿を取得できませんでした: "
                    f"{_error_text(navigation_error)}"
                ) from navigation_error
            raise scraper.ScrapingError(
                f"ブラウザページから投稿を取得できませんでした。status={status}"
            )

        links = refresh_fix._extract_pager_links(
            soup,
            target_url,
            final_url,
        )
        status = getattr(response, "status", None) if response is not None else None
        return posts, final_url, links, status, navigation_error
    finally:
        try:
            page.close()
        except Exception:
            pass


def _crawl_with_browser(
    url: str,
    max_pages: int = 20,
    stop_at_post_no: Optional[int] = None,
):
    root_url = refresh_fix._thread_root(url)
    if not root_url:
        raise scraper.ScrapingError("スレURLが空です。")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise scraper.ScrapingError(
            f"Playwrightを読み込めませんでした: {_error_text(exc)}"
        ) from exc

    safe_max_pages = min(max(1, int(max_pages)), 20)
    queue = deque([(root_url, None)])
    seen_targets: set[str] = set()
    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    all_posts = []
    trace: list[str] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            },
            viewport={"width": 1280, "height": 720},
        )
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
        except Exception:
            pass

        try:
            while queue and len(seen_targets) < safe_max_pages:
                target_url, source_url = queue.popleft()
                normalized_target = refresh_fix._without_fragment(target_url)
                if normalized_target in seen_targets:
                    continue
                seen_targets.add(normalized_target)

                try:
                    posts, final_url, links, status, navigation_error = _read_page(
                        context,
                        normalized_target,
                        source_url,
                    )
                except Exception as exc:
                    detail = _error_text(exc)
                    trace.append(
                        f"{refresh_fix._trace_path(normalized_target)} error={detail}"
                    )
                    _LOGGER.warning(
                        "[THREAD_BROWSER][page_error] target_url=%s source_url=%s error=%s",
                        normalized_target,
                        source_url,
                        detail,
                    )
                    if len(seen_targets) == 1:
                        raise scraper.ScrapingError(
                            f"ブラウザ取得に失敗しました: {detail}"
                        ) from exc
                    continue

                min_no, max_no, count = refresh_fix._number_range(posts)
                link_paths = ",".join(
                    refresh_fix._trace_path(link) for link in links
                ) or "-"
                navigation_note = (
                    f" nav_error={_error_text(navigation_error)}"
                    if navigation_error is not None
                    else ""
                )
                trace.append(
                    f"{refresh_fix._trace_path(normalized_target)}"
                    f"->{refresh_fix._trace_path(final_url)} "
                    f"status={status} count={count} range={min_no}-{max_no} "
                    f"links={link_paths}{navigation_note}"
                )
                _LOGGER.info(
                    "[THREAD_BROWSER][page] target_url=%s source_url=%s final_url=%s "
                    "status=%s count=%s min_no=%s max_no=%s pager_links=%s "
                    "navigation_error=%s",
                    normalized_target,
                    source_url,
                    final_url,
                    status,
                    count,
                    min_no,
                    max_no,
                    len(links),
                    _error_text(navigation_error)
                    if navigation_error is not None
                    else "-",
                )

                _merge_posts(all_posts, posts, seen_post_nos, seen_unknown)

                for link in links:
                    normalized_link = refresh_fix._without_fragment(link)
                    if normalized_link not in seen_targets and all(
                        normalized_link != queued_url for queued_url, _ in queue
                    ):
                        queue.append((normalized_link, final_url))
        finally:
            try:
                context.close()
            finally:
                browser.close()

    if not all_posts:
        raise scraper.ScrapingError("ブラウザでも投稿を取得できませんでした。")

    numbered = [
        post for post in all_posts if getattr(post, "post_no", None) is not None
    ]
    unknown = [
        post for post in all_posts if getattr(post, "post_no", None) is None
    ]
    numbered.sort(key=lambda post: int(post.post_no))
    return refresh_fix.CrawlPosts(numbered + unknown, trace=trace)


def install_thread_refresh_browser_fallback() -> None:
    """全件取得が不完全な場合だけPlaywrightで再取得する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch = services.fetch_posts_from_thread

    def fetch_with_browser_fallback(
        url: str,
        max_pages: int = 20,
        stop_at_post_no: Optional[int] = None,
    ):
        result = original_fetch(
            url,
            max_pages=max_pages,
            stop_at_post_no=stop_at_post_no,
        )
        if stop_at_post_no is not None or _has_oldest(result):
            return result

        _LOGGER.warning(
            "[THREAD_BROWSER][fallback_start] url=%s requests_count=%s",
            url,
            len(result),
        )
        try:
            browser_result = _crawl_with_browser(
                url,
                max_pages=max_pages,
                stop_at_post_no=stop_at_post_no,
            )
        except Exception as exc:
            _LOGGER.warning(
                "[THREAD_BROWSER][fallback_failed] url=%s error=%s",
                url,
                _error_text(exc),
            )
            return result

        if _has_oldest(browser_result):
            _LOGGER.info(
                "[THREAD_BROWSER][fallback_success] url=%s count=%s",
                url,
                len(browser_result),
            )
            return browser_result

        _LOGGER.warning(
            "[THREAD_BROWSER][fallback_incomplete] url=%s count=%s trace=%s",
            url,
            len(browser_result),
            " | ".join(getattr(browser_result, "trace", []) or []),
        )
        return browser_result if len(browser_result) > len(result) else result

    scraper.fetch_posts_from_thread = fetch_with_browser_fallback
    services.fetch_posts_from_thread = fetch_with_browser_fallback
    _INSTALLED = True
