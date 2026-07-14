# thread_refresh_fix.py
"""爆サイのHTML内ページャーをたどって全レスを取得する恒久修正。"""

from __future__ import annotations

import logging
import random
import re
import time
from collections import deque
from typing import Iterable, Optional
from urllib.parse import urljoin, urlsplit, urlunsplit

import scraper
import services


_INSTALLED = False
_LOGGER = logging.getLogger(__name__)


def _thread_id(url: str) -> Optional[str]:
    match = re.search(r"(?:^|/)tid=(\d+)(?:/|$)", url or "")
    return match.group(1) if match else None


def _without_fragment(url: str) -> str:
    parts = urlsplit((url or "").strip())
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def _thread_root(url: str) -> str:
    """ページ移動用のp・tp・ttgid・rridを除いた元スレURLへ統一する。"""
    raw = _without_fragment(url)
    if not raw:
        return ""

    scheme_marker = "__SCHEME_SLASHES__"
    base = raw.replace("://", scheme_marker)
    base = re.sub(r"/(?:p|tp|ttgid|rrid)=\d+", "", base)
    base = re.sub(r"/{2,}", "/", base)
    base = base.replace(scheme_marker, "://")
    if not base.endswith("/"):
        base += "/"
    return base


def _is_same_thread_page(original_url: str, candidate_url: str) -> bool:
    original_tid = _thread_id(original_url)
    candidate_tid = _thread_id(candidate_url)
    if not original_tid or original_tid != candidate_tid:
        return False

    parts = urlsplit(candidate_url)
    if parts.scheme != "https" or parts.netloc.lower() not in {
        "bakusai.com",
        "www.bakusai.com",
    }:
        return False

    path = parts.path
    if not path.startswith(("/thr_res/", "/thr_repo02/")):
        return False
    return bool(re.search(r"/p=\d+(?:/|$)", path))


def _page_number(url: str) -> int:
    match = re.search(r"/p=(\d+)(?:/|$)", url or "")
    return int(match.group(1)) if match else 10**9


def _extract_pager_links(soup, original_url: str, response_url: str) -> list[str]:
    """HTMLに実際に含まれる同一スレッドのページ送りURLだけを返す。"""
    links: list[str] = []
    seen_urls: set[str] = set()
    seen_pages: set[tuple[str, int]] = set()

    for node in soup.select("a[href]"):
        href = (node.get("href") or "").strip()
        if not href:
            continue
        absolute = _without_fragment(urljoin(response_url or original_url, href))
        if absolute in seen_urls or not _is_same_thread_page(original_url, absolute):
            continue

        path = urlsplit(absolute).path
        endpoint = "thr_repo02" if path.startswith("/thr_repo02/") else "thr_res"
        page_identity = (endpoint, _page_number(absolute))
        if page_identity in seen_pages:
            continue

        seen_urls.add(absolute)
        seen_pages.add(page_identity)
        links.append(absolute)

    links.sort(key=lambda value: (_page_number(value), value))
    return links


def _number_range(posts: Iterable[object]) -> tuple[Optional[int], Optional[int], int]:
    post_list = list(posts)
    numbers = [
        int(post.post_no)
        for post in post_list
        if getattr(post, "post_no", None) is not None
    ]
    if not numbers:
        return None, None, len(post_list)
    return min(numbers), max(numbers), len(post_list)


def _fetch_page(session, url: str, headers: dict):
    """1ページを取得し、投稿とHTML内ページャーリンクを返す。"""
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            response = session.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                raise scraper.ScrapingError(
                    f"HTTPステータスコードが異常です: {response.status_code}"
                )

            soup = scraper.BeautifulSoup(response.text or "", "html.parser")
            posts = scraper._parse_posts_from_soup(soup)
            if posts:
                final_url = getattr(response, "url", "") or url
                links = _extract_pager_links(soup, url, final_url)
                return posts, final_url, links
            last_error = scraper.ScrapingError(
                "投稿らしきテキストが見つかりませんでした。"
            )
        except Exception as exc:
            last_error = exc

        if attempt < 2:
            time.sleep(0.6 * (attempt + 1) + random.uniform(0.0, 0.4))

    if isinstance(last_error, scraper.ScrapingError):
        raise last_error
    raise scraper.ScrapingError(f"ページ取得に失敗しました: {last_error}")


def _crawl_thread_pages(
    url: str,
    max_pages: int = 20,
    stop_at_post_no: Optional[int] = None,
):
    """
    URLを推測せず、爆サイがHTML内に出した同一スレッドのページャーをたどる。

    stop_at_post_noがある通常更新でも、最新側を取りこぼさないようページャー巡回を
    完了してから重複を除去する。
    """
    root_url = _thread_root(url)
    if not root_url:
        raise scraper.ScrapingError("スレURLが空です。")

    safe_max_pages = min(max(1, int(max_pages)), 20)
    session = scraper.requests.Session()
    headers = scraper._build_headers()

    queue = deque([root_url])
    seen_urls: set[str] = set()
    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    all_posts = []

    while queue and len(seen_urls) < safe_max_pages:
        request_url = queue.popleft()
        normalized_url = _without_fragment(request_url)
        if normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        try:
            posts, final_url, links = _fetch_page(session, normalized_url, headers)
        except scraper.ScrapingError:
            if len(seen_urls) == 1:
                raise
            continue

        min_no, max_no, count = _number_range(posts)
        _LOGGER.info(
            "[THREAD_REFRESH][page] request_url=%s final_url=%s count=%s "
            "min_no=%s max_no=%s pager_links=%s",
            normalized_url,
            final_url,
            count,
            min_no,
            max_no,
            len(links),
        )

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
            all_posts.append(post)

        for link in links:
            if link not in seen_urls and link not in queue:
                queue.append(link)

        time.sleep(random.uniform(0.1, 0.2))

    if not all_posts:
        raise scraper.ScrapingError("投稿らしきテキストが見つかりませんでした。")

    numbered = [post for post in all_posts if getattr(post, "post_no", None) is not None]
    unknown = [post for post in all_posts if getattr(post, "post_no", None) is None]
    numbered.sort(key=lambda post: int(post.post_no))
    return numbered + unknown


def _guarded_refresh_cached_thread(
    db,
    thread_url: str,
    *,
    full_refresh: bool,
) -> None:
    """#1未到達の全件取得を成功扱いせず、古いキャッシュを維持する。"""
    stop_at_post_no = None if full_refresh else services._max_cached_post_no(db, thread_url)
    effective_full_refresh = full_refresh or stop_at_post_no is None

    post_list = list(
        services.fetch_posts_from_thread(
            thread_url,
            stop_at_post_no=stop_at_post_no,
        )
    )

    if effective_full_refresh and not any(
        getattr(post, "post_no", None) == 1 for post in post_list
    ):
        numbers = [
            int(post.post_no)
            for post in post_list
            if getattr(post, "post_no", None) is not None
        ]
        _LOGGER.warning(
            "[THREAD_REFRESH][incomplete] url=%s count=%s min_no=%s max_no=%s",
            thread_url,
            len(post_list),
            min(numbers) if numbers else None,
            max(numbers) if numbers else None,
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


def install_thread_refresh_fix() -> None:
    """HTML内ページャー巡回と不完全更新防止を起動時に有効化する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    scraper.fetch_posts_from_thread = _crawl_thread_pages
    services.fetch_posts_from_thread = _crawl_thread_pages
    services._refresh_cached_thread = _guarded_refresh_cached_thread

    _INSTALLED = True
