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
_FETCH_BATCH_SIZE = 3
_FETCH_TIMEOUT_MS = 15_000
_URL_ATTEMPT_MULTIPLIER = 2


def _has_oldest(posts) -> bool:
    return any(getattr(post, "post_no", None) == 1 for post in posts)


def _merge_posts(target, posts, seen_post_nos, seen_unknown) -> int:
    """新しく追加したレス数を返す。"""
    added = 0
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
        added += 1
    return added


def _count_new_numbered(posts, seen_post_nos) -> int:
    numbers = {
        int(post.post_no)
        for post in posts
        if getattr(post, "post_no", None) is not None
    }
    return len(numbers - seen_post_nos)


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


def _navigate_and_parse(page, target_url: str, source_url: Optional[str]):
    response = None
    navigation_error: Optional[Exception] = None

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

    links = refresh_fix._extract_pager_links(soup, target_url, final_url)
    status = getattr(response, "status", None) if response is not None else None
    return posts, final_url, links, status, navigation_error


def _read_page(context, target_url: str, source_url: Optional[str]):
    """同じブラウザコンテキスト内の新しいページでURLを直接開く。"""
    page = context.new_page()
    _configure_page(page)
    try:
        return _navigate_and_parse(page, target_url, source_url)
    finally:
        try:
            page.close()
        except Exception:
            pass


def _fetch_html_batch(page, targets: list[tuple[str, Optional[str]]]) -> list[dict]:
    """描画済みページ内から、同一オリジンのHTMLを3ページずつ並行取得する。"""
    payload = [
        {"url": target_url, "sourceUrl": source_url or ""}
        for target_url, source_url in targets
    ]
    result = page.evaluate(
        """
        async ({items, timeoutMs}) => {
            return await Promise.all(items.map(async (item) => {
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), timeoutMs);
                try {
                    const response = await fetch(item.url, {
                        method: 'GET',
                        credentials: 'include',
                        redirect: 'follow',
                        cache: 'no-store',
                        referrer: item.sourceUrl || window.location.href,
                        referrerPolicy: 'strict-origin-when-cross-origin',
                        headers: {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                        },
                        signal: controller.signal
                    });
                    const html = await response.text();
                    return {
                        requestedUrl: item.url,
                        finalUrl: response.url || item.url,
                        status: response.status,
                        html,
                        error: ''
                    };
                } catch (error) {
                    return {
                        requestedUrl: item.url,
                        finalUrl: '',
                        status: 0,
                        html: '',
                        error: String(error)
                    };
                } finally {
                    clearTimeout(timer);
                }
            }));
        }
        """,
        {"items": payload, "timeoutMs": _FETCH_TIMEOUT_MS},
    )
    return list(result or [])


def _parse_fetched_html(result: dict, target_url: str):
    error = (result.get("error") or "").strip()
    if error:
        raise scraper.ScrapingError(f"browser fetch failed: {error}")

    html = result.get("html") or ""
    final_url = result.get("finalUrl") or target_url
    status = result.get("status")
    soup = scraper.BeautifulSoup(html, "html.parser")
    posts = scraper._parse_posts_from_soup(soup)
    if not posts:
        raise scraper.ScrapingError(
            f"browser fetchで投稿を取得できませんでした。status={status}"
        )
    links = refresh_fix._extract_pager_links(soup, target_url, final_url)
    return posts, final_url, links, status, None


def _append_trace_and_log(
    *,
    trace: list[str],
    target_url: str,
    source_url: Optional[str],
    final_url: str,
    status,
    posts,
    links,
    mode: str,
    navigation_error: Optional[Exception],
    new_numbered: int,
    unique_page_count: int,
    attempt_count: int,
) -> None:
    min_no, max_no, count = refresh_fix._number_range(posts)
    link_paths = ",".join(refresh_fix._trace_path(link) for link in links) or "-"
    navigation_note = (
        f" nav_error={_error_text(navigation_error)}"
        if navigation_error is not None
        else ""
    )
    trace.append(
        f"{refresh_fix._trace_path(target_url)}"
        f"->{refresh_fix._trace_path(final_url)} "
        f"mode={mode} status={status} count={count} range={min_no}-{max_no} "
        f"new={new_numbered} unique_pages={unique_page_count} "
        f"attempts={attempt_count} links={link_paths}{navigation_note}"
    )
    _LOGGER.info(
        "[THREAD_BROWSER][page] mode=%s target_url=%s source_url=%s final_url=%s "
        "status=%s count=%s min_no=%s max_no=%s new_numbered=%s "
        "unique_pages=%s attempts=%s pager_links=%s navigation_error=%s",
        mode,
        target_url,
        source_url,
        final_url,
        status,
        count,
        min_no,
        max_no,
        new_numbered,
        unique_page_count,
        attempt_count,
        len(links),
        _error_text(navigation_error) if navigation_error is not None else "-",
    )


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
    safe_max_attempts = max(
        safe_max_pages + 1,
        safe_max_pages * _URL_ATTEMPT_MULTIPLIER,
    )
    queue: deque[tuple[str, Optional[str]]] = deque()
    seen_targets: set[str] = set()
    seen_page_numbers: set[int] = set()
    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    all_posts = []
    trace: list[str] = []
    unique_page_count = 0

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

        host_page = context.new_page()
        _configure_page(host_page)

        try:
            root_posts, root_final_url, root_links, root_status, root_nav_error = (
                _navigate_and_parse(host_page, root_url, None)
            )
            seen_targets.add(refresh_fix._without_fragment(root_url))
            root_page_no = refresh_fix._page_number(root_final_url)
            if root_page_no < 10**9:
                seen_page_numbers.add(root_page_no)

            root_new_numbered = _count_new_numbered(root_posts, seen_post_nos)
            _merge_posts(all_posts, root_posts, seen_post_nos, seen_unknown)
            if root_new_numbered > 0:
                unique_page_count += 1

            _append_trace_and_log(
                trace=trace,
                target_url=root_url,
                source_url=None,
                final_url=root_final_url,
                status=root_status,
                posts=root_posts,
                links=root_links,
                mode="navigation",
                navigation_error=root_nav_error,
                new_numbered=root_new_numbered,
                unique_page_count=unique_page_count,
                attempt_count=len(seen_targets),
            )

            if 1 not in seen_post_nos:
                for link in root_links:
                    normalized_link = refresh_fix._without_fragment(link)
                    page_no = refresh_fix._page_number(normalized_link)
                    if normalized_link in seen_targets or page_no in seen_page_numbers:
                        continue
                    if any(refresh_fix._page_number(item[0]) == page_no for item in queue):
                        continue
                    queue.append((normalized_link, root_final_url))

            while (
                queue
                and unique_page_count < safe_max_pages
                and len(seen_targets) < safe_max_attempts
                and 1 not in seen_post_nos
            ):
                batch: list[tuple[str, Optional[str]]] = []
                while (
                    queue
                    and len(batch) < _FETCH_BATCH_SIZE
                    and len(seen_targets) + len(batch) < safe_max_attempts
                ):
                    target_url, source_url = queue.popleft()
                    normalized_target = refresh_fix._without_fragment(target_url)
                    page_no = refresh_fix._page_number(normalized_target)
                    if normalized_target in seen_targets or page_no in seen_page_numbers:
                        continue
                    if any(refresh_fix._page_number(item[0]) == page_no for item in batch):
                        continue
                    batch.append((normalized_target, source_url))

                if not batch:
                    continue

                for target_url, _ in batch:
                    seen_targets.add(target_url)
                    page_no = refresh_fix._page_number(target_url)
                    if page_no < 10**9:
                        seen_page_numbers.add(page_no)

                try:
                    fetched_results = _fetch_html_batch(host_page, batch)
                except Exception as exc:
                    _LOGGER.warning(
                        "[THREAD_BROWSER][batch_error] count=%s error=%s",
                        len(batch),
                        _error_text(exc),
                    )
                    fetched_results = []

                by_requested = {
                    refresh_fix._without_fragment(item.get("requestedUrl") or ""): item
                    for item in fetched_results
                    if item.get("requestedUrl")
                }

                for target_url, source_url in batch:
                    result = by_requested.get(target_url)
                    mode = "fetch"
                    try:
                        if result is None:
                            raise scraper.ScrapingError("browser fetch result missing")
                        posts, final_url, links, status, navigation_error = (
                            _parse_fetched_html(result, target_url)
                        )
                        if _count_new_numbered(posts, seen_post_nos) == 0:
                            raise scraper.ScrapingError(
                                "browser fetch returned no new numbered posts"
                            )
                    except Exception as fetch_exc:
                        _LOGGER.info(
                            "[THREAD_BROWSER][fetch_fallback] target_url=%s reason=%s",
                            target_url,
                            _error_text(fetch_exc),
                        )
                        mode = "navigation"
                        try:
                            posts, final_url, links, status, navigation_error = _read_page(
                                context,
                                target_url,
                                source_url,
                            )
                        except Exception as exc:
                            detail = _error_text(exc)
                            trace.append(
                                f"{refresh_fix._trace_path(target_url)} error={detail}"
                            )
                            _LOGGER.warning(
                                "[THREAD_BROWSER][page_error] target_url=%s "
                                "source_url=%s error=%s",
                                target_url,
                                source_url,
                                detail,
                            )
                            continue

                    new_numbered = _count_new_numbered(posts, seen_post_nos)
                    _merge_posts(all_posts, posts, seen_post_nos, seen_unknown)
                    if new_numbered > 0:
                        unique_page_count += 1

                    _append_trace_and_log(
                        trace=trace,
                        target_url=target_url,
                        source_url=source_url,
                        final_url=final_url,
                        status=status,
                        posts=posts,
                        links=links,
                        mode=mode,
                        navigation_error=navigation_error,
                        new_numbered=new_numbered,
                        unique_page_count=unique_page_count,
                        attempt_count=len(seen_targets),
                    )

                    if 1 in seen_post_nos or unique_page_count >= safe_max_pages:
                        break

                    for link in links:
                        normalized_link = refresh_fix._without_fragment(link)
                        page_no = refresh_fix._page_number(normalized_link)
                        if normalized_link in seen_targets or page_no in seen_page_numbers:
                            continue
                        if any(
                            refresh_fix._page_number(queued_url) == page_no
                            for queued_url, _ in queue
                        ):
                            continue
                        queue.append((normalized_link, final_url))
        finally:
            try:
                host_page.close()
            except Exception:
                pass
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

        _LOGGER.info(
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
