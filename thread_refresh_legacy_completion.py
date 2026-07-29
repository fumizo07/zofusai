"""古い爆サイスレッドで先頭ページだけを全件扱いする誤判定を補正する。"""

from __future__ import annotations

import logging
import math
import re
from typing import Iterable, Optional

import scraper
import services
import thread_refresh_browser as browser_refresh
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False
_MAX_PAGES = 20


def _extract_declared_post_count(html: str) -> Optional[int]:
    """ページタイトル等の「75レス」からスレッド総レス数を取得する。"""
    soup = scraper.BeautifulSoup(html or "", "html.parser")
    candidates: list[str] = []

    if soup.title:
        candidates.append(soup.title.get_text(" ", strip=True))

    for selector, attr in (
        ("meta[property='og:title'][content]", "content"),
        ("meta[name='twitter:title'][content]", "content"),
    ):
        node = soup.select_one(selector)
        if node is not None:
            value = (node.get(attr) or "").strip()
            if value:
                candidates.append(value)

    for text in candidates:
        match = re.search(r"(?:^|[｜|])\s*(\d{1,4})\s*レス(?:[｜|]|$)", text)
        if match is None:
            match = re.search(r"(\d{1,4})\s*レス", text)
        if match is None:
            continue
        try:
            count = int(match.group(1))
        except (TypeError, ValueError):
            continue
        if count > 0:
            return count

    return None


def _numbered_values(posts: Iterable[object]) -> set[int]:
    return {
        int(post.post_no)
        for post in posts
        if getattr(post, "post_no", None) is not None
    }


def _merge_posts(target: list[object], posts: Iterable[object]) -> int:
    """レス番号または本文・日時で重複を避け、追加数を返す。"""
    seen_numbers = _numbered_values(target)
    seen_unknown = {
        (
            getattr(post, "posted_at", None),
            getattr(post, "body", "") or "",
        )
        for post in target
        if getattr(post, "post_no", None) is None
    }
    added = 0

    for post in posts:
        post_no = getattr(post, "post_no", None)
        if post_no is not None:
            number = int(post_no)
            if number in seen_numbers:
                continue
            seen_numbers.add(number)
        else:
            key = (
                getattr(post, "posted_at", None),
                getattr(post, "body", "") or "",
            )
            if key in seen_unknown:
                continue
            seen_unknown.add(key)

        target.append(post)
        added += 1

    return added


def _inspect_declared_count(url: str) -> Optional[int]:
    """軽量な通常リクエストで総レス数だけ確認する。"""
    try:
        response = scraper.requests.get(
            url,
            headers=scraper._build_headers(),
            timeout=10,
        )
    except Exception as exc:
        _LOGGER.info(
            "[THREAD_LEGACY][count_check_failed] url=%s error=%s",
            url,
            browser_refresh._error_text(exc),
        )
        return None

    if response.status_code != 200:
        _LOGGER.info(
            "[THREAD_LEGACY][count_check_failed] url=%s status=%s",
            url,
            response.status_code,
        )
        return None

    return _extract_declared_post_count(response.text or "")


def _page_target(root_url: str, page_no: int) -> str:
    return f"{root_url}p={page_no}/tp=1/"


def _browser_context(playwright):
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
    return browser, context


def _complete_declared_posts_with_browser(
    url: str,
    seed_posts: Iterable[object],
    declared_count: int,
    *,
    max_pages: int,
):
    """総レス数へ届くまでp=2以降を同一ブラウザコンテキストで補完する。"""
    root_url = refresh_fix._thread_root(url)
    if not root_url:
        raise scraper.ScrapingError("スレURLが空です。")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise scraper.ScrapingError(
            f"Playwrightを読み込めませんでした: {browser_refresh._error_text(exc)}"
        ) from exc

    merged = list(seed_posts)
    initial_numbers = _numbered_values(merged)
    initial_max = max(initial_numbers) if initial_numbers else None
    safe_max_pages = min(max(1, int(max_pages)), _MAX_PAGES)
    trace = list(getattr(seed_posts, "trace", []) or [])
    page_failures: list[int] = []

    with sync_playwright() as playwright:
        browser, context = _browser_context(playwright)
        host_page = context.new_page()
        browser_refresh._configure_page(host_page)
        try:
            root_posts, root_final_url, root_links, _, _ = (
                browser_refresh._navigate_and_parse(host_page, root_url, None)
            )
            _merge_posts(merged, root_posts)

            root_number_count = len(_numbered_values(root_posts))
            seed_number_count = len(initial_numbers)
            page_size = max(root_number_count, seed_number_count, 1)
            expected_pages = min(
                safe_max_pages,
                max(1, int(math.ceil(declared_count / page_size))),
            )

            actual_by_page = {
                refresh_fix._page_number(link): link
                for link in root_links
                if 2 <= refresh_fix._page_number(link) <= expected_pages
            }
            targets = [
                (
                    actual_by_page.get(page_no)
                    or _page_target(root_url, page_no),
                    root_final_url,
                    page_no,
                )
                for page_no in range(2, expected_pages + 1)
            ]

            for offset in range(0, len(targets), browser_refresh._FETCH_BATCH_SIZE):
                numbers = _numbered_values(merged)
                if numbers and max(numbers) >= declared_count:
                    break

                batch_items = targets[
                    offset : offset + browser_refresh._FETCH_BATCH_SIZE
                ]
                request_batch = [
                    (target_url, source_url)
                    for target_url, source_url, _ in batch_items
                ]

                try:
                    fetched_results = browser_refresh._fetch_html_batch(
                        host_page,
                        request_batch,
                    )
                except Exception as exc:
                    _LOGGER.warning(
                        "[THREAD_LEGACY][batch_error] url=%s pages=%s error=%s",
                        url,
                        ",".join(str(item[2]) for item in batch_items),
                        browser_refresh._error_text(exc),
                    )
                    fetched_results = []

                by_requested = {
                    refresh_fix._without_fragment(item.get("requestedUrl") or ""): item
                    for item in fetched_results
                    if item.get("requestedUrl")
                }

                for target_url, source_url, page_no in batch_items:
                    before_numbers = _numbered_values(merged)
                    result = by_requested.get(
                        refresh_fix._without_fragment(target_url)
                    )
                    mode = "fetch"
                    try:
                        if result is None:
                            raise scraper.ScrapingError(
                                "browser fetch result missing"
                            )
                        posts, final_url, _, status, _ = (
                            browser_refresh._parse_fetched_html(result, target_url)
                        )
                        if not (_numbered_values(posts) - before_numbers):
                            raise scraper.ScrapingError(
                                "browser fetch returned no new numbered posts"
                            )
                    except Exception as fetch_exc:
                        mode = "navigation"
                        try:
                            posts, final_url, _, status, _ = browser_refresh._read_page(
                                context,
                                target_url,
                                source_url,
                            )
                            if not (_numbered_values(posts) - before_numbers):
                                raise scraper.ScrapingError(
                                    "browser navigation returned no new numbered posts"
                                )
                        except Exception as nav_exc:
                            page_failures.append(page_no)
                            detail = browser_refresh._error_text(nav_exc)
                            trace.append(
                                f"{refresh_fix._trace_path(target_url)} error={detail}"
                            )
                            _LOGGER.warning(
                                "[THREAD_LEGACY][page_error] url=%s page=%s "
                                "fetch_error=%s navigation_error=%s",
                                url,
                                page_no,
                                browser_refresh._error_text(fetch_exc),
                                detail,
                            )
                            continue

                    added = _merge_posts(merged, posts)
                    current_numbers = _numbered_values(merged)
                    current_max = max(current_numbers) if current_numbers else None
                    trace.append(
                        f"{refresh_fix._trace_path(target_url)}"
                        f"->{refresh_fix._trace_path(final_url)} "
                        f"mode={mode} status={status} added={added} "
                        f"max_no={current_max} declared={declared_count}"
                    )
                    _LOGGER.info(
                        "[THREAD_LEGACY][page] url=%s page=%s mode=%s "
                        "added=%s max_no=%s declared=%s",
                        url,
                        page_no,
                        mode,
                        added,
                        current_max,
                        declared_count,
                    )
        finally:
            try:
                host_page.close()
            except Exception:
                pass
            try:
                context.close()
            finally:
                browser.close()

    numbered = _numbered_values(merged)
    final_min = min(numbered) if numbered else None
    final_max = max(numbered) if numbered else None
    if final_min != 1 or final_max is None or final_max < declared_count:
        _LOGGER.warning(
            "[THREAD_LEGACY][completion_incomplete] url=%s declared=%s "
            "initial_max=%s final_min=%s final_max=%s failures=%s trace=%s",
            url,
            declared_count,
            initial_max,
            final_min,
            final_max,
            ",".join(str(page) for page in page_failures) or "-",
            " | ".join(trace) if trace else "-",
        )
        raise scraper.ScrapingError(
            "ページタイトルの総レス数まで取得できなかったため、"
            "キャッシュ更新を完了扱いにしません。"
        )

    merged_numbered = [
        post for post in merged if getattr(post, "post_no", None) is not None
    ]
    merged_unknown = [
        post for post in merged if getattr(post, "post_no", None) is None
    ]
    merged_numbered.sort(key=lambda post: int(post.post_no))
    _LOGGER.info(
        "[THREAD_LEGACY][completion_success] url=%s declared=%s count=%s max_no=%s",
        url,
        declared_count,
        len(merged_numbered) + len(merged_unknown),
        final_max,
    )
    return refresh_fix.CrawlPosts(merged_numbered + merged_unknown, trace=trace)


def install_legacy_thread_completion() -> None:
    """#1を含む先頭ページだけの取得を、総レス数との照合で補完する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch = services.fetch_posts_from_thread

    def fetch_with_declared_count_completion(
        url: str,
        max_pages: int = 20,
        stop_at_post_no: Optional[int] = None,
    ):
        result = original_fetch(
            url,
            max_pages=max_pages,
            stop_at_post_no=stop_at_post_no,
        )
        if stop_at_post_no is not None:
            return result

        numbers = _numbered_values(result)
        if not numbers or min(numbers) != 1:
            return result

        declared_count = _inspect_declared_count(url)
        if declared_count is None or max(numbers) >= declared_count:
            return result

        _LOGGER.warning(
            "[THREAD_LEGACY][completion_start] url=%s declared=%s "
            "cached_count=%s min_no=%s max_no=%s",
            url,
            declared_count,
            len(numbers),
            min(numbers),
            max(numbers),
        )
        return _complete_declared_posts_with_browser(
            url,
            result,
            declared_count,
            max_pages=max_pages,
        )

    scraper.fetch_posts_from_thread = fetch_with_declared_count_completion
    services.fetch_posts_from_thread = fetch_with_declared_count_completion
    _INSTALLED = True
