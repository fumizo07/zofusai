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
_URL_ATTEMPT_MULTIPLIER = 2


class CrawlPosts(list):
    """取得失敗時の診断用に巡回履歴を保持する投稿リスト。"""

    def __init__(self, values=(), *, trace: Optional[list[str]] = None):
        super().__init__(values)
        self.trace = list(trace or [])


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
    """同じスレッドの通常ページャーだけを許可する。"""
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
    if not path.startswith("/thr_res/"):
        return False
    if re.search(r"/rrid=\d+(?:/|$)", path):
        return False
    return bool(re.search(r"/p=\d+(?:/|$)", path))


def _page_number(url: str) -> int:
    match = re.search(r"/p=(\d+)(?:/|$)", url or "")
    return int(match.group(1)) if match else 10**9


def _pager_priority(url: str) -> tuple[int, int, str]:
    """同じページ番号ならtp=1付きの通常リンクを優先する。"""
    path = urlsplit(url).path
    has_tp = bool(re.search(r"/tp=1(?:/|$)", path))
    has_ttgid = bool(re.search(r"/ttgid=\d+(?:/|$)", path))
    return (0 if has_tp else 1, 1 if has_ttgid else 0, url)


def _extract_pager_links(soup, original_url: str, response_url: str) -> list[str]:
    """HTMLに実際に含まれる通常ページ送りURLをページ番号ごとに返す。"""
    best_by_page: dict[int, str] = {}

    for node in soup.select("a[href], option[value], form[action]"):
        value = ""
        for attr in ("href", "value", "action"):
            value = (node.get(attr) or "").strip()
            if value:
                break
        if not value:
            continue

        absolute = _without_fragment(urljoin(response_url or original_url, value))
        if not _is_same_thread_page(original_url, absolute):
            continue

        page_no = _page_number(absolute)
        current = best_by_page.get(page_no)
        if current is None or _pager_priority(absolute) < _pager_priority(current):
            best_by_page[page_no] = absolute

    return [best_by_page[page] for page in sorted(best_by_page)]


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


def _trace_path(url: str) -> str:
    parts = urlsplit(url or "")
    path = parts.path or "/"
    return f"{path}?{parts.query}" if parts.query else path


def _merge_posts(
    target,
    posts,
    seen_post_nos: set[int],
    seen_unknown: set[tuple[Optional[str], str]],
) -> tuple[int, int]:
    """新規投稿数と、新しいレス番号を含む投稿数を返す。"""
    added = 0
    added_numbered = 0

    for post in posts:
        post_no = getattr(post, "post_no", None)
        if post_no is not None:
            post_no = int(post_no)
            if post_no in seen_post_nos:
                continue
            seen_post_nos.add(post_no)
            added_numbered += 1
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

    return added, added_numbered


def _fetch_page(
    session,
    url: str,
    headers: dict,
    *,
    referer: Optional[str] = None,
):
    """1ページを取得し、投稿とHTML内の通常ページャーリンクを返す。"""
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            request_headers = dict(headers or {})
            if referer:
                request_headers["Referer"] = referer

            response = session.get(url, headers=request_headers, timeout=10)
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
    爆サイがHTML内に出した通常ページャーだけを、同じセッションでたどる。

    特定レス用rridリンクと投稿用thr_repo02リンクはページャーではないため除外する。
    リンク遷移時は実ブラウザと同様に直前ページをRefererとして送る。
    URLが異なっても同じレス範囲なら実ページ上限を消費しない。
    """
    root_url = _thread_root(url)
    if not root_url:
        raise scraper.ScrapingError("スレURLが空です。")

    safe_max_pages = min(max(1, int(max_pages)), 20)
    safe_max_attempts = max(
        safe_max_pages + 1,
        safe_max_pages * _URL_ATTEMPT_MULTIPLIER,
    )
    session = scraper.requests.Session()
    headers = scraper._build_headers()

    queue = deque([(root_url, None)])
    seen_urls: set[str] = set()
    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    all_posts = []
    trace: list[str] = []
    unique_page_count = 0

    while (
        queue
        and unique_page_count < safe_max_pages
        and len(seen_urls) < safe_max_attempts
    ):
        request_url, referer = queue.popleft()
        normalized_url = _without_fragment(request_url)
        if normalized_url in seen_urls:
            continue
        seen_urls.add(normalized_url)

        try:
            posts, final_url, links = _fetch_page(
                session,
                normalized_url,
                headers,
                referer=referer,
            )
        except scraper.ScrapingError as exc:
            trace.append(f"{_trace_path(normalized_url)} error={exc}")
            if len(seen_urls) == 1:
                raise
            continue

        _, new_numbered = _merge_posts(
            all_posts,
            posts,
            seen_post_nos,
            seen_unknown,
        )
        if new_numbered > 0:
            unique_page_count += 1

        min_no, max_no, count = _number_range(posts)
        link_paths = ",".join(_trace_path(link) for link in links) or "-"
        trace.append(
            f"{_trace_path(normalized_url)}->{_trace_path(final_url)} "
            f"count={count} range={min_no}-{max_no} new={new_numbered} "
            f"unique_pages={unique_page_count} links={link_paths}"
        )
        _LOGGER.info(
            "[THREAD_REFRESH][page] request_url=%s final_url=%s referer=%s count=%s "
            "min_no=%s max_no=%s new_numbered=%s unique_pages=%s attempts=%s "
            "pager_links=%s",
            normalized_url,
            final_url,
            referer,
            count,
            min_no,
            max_no,
            new_numbered,
            unique_page_count,
            len(seen_urls),
            len(links),
        )

        if 1 in seen_post_nos:
            break

        for link in links:
            normalized_link = _without_fragment(link)
            if normalized_link not in seen_urls and all(
                normalized_link != queued_url for queued_url, _ in queue
            ):
                queue.append((normalized_link, final_url))

        time.sleep(random.uniform(0.1, 0.2))

    if not all_posts:
        raise scraper.ScrapingError("投稿らしきテキストが見つかりませんでした。")

    numbered = [post for post in all_posts if getattr(post, "post_no", None) is not None]
    unknown = [post for post in all_posts if getattr(post, "post_no", None) is None]
    numbered.sort(key=lambda post: int(post.post_no))
    return CrawlPosts(numbered + unknown, trace=trace)


def _guarded_refresh_cached_thread(
    db,
    thread_url: str,
    *,
    full_refresh: bool,
) -> None:
    """#1未到達の全件取得を成功扱いせず、古いキャッシュを維持する。"""
    stop_at_post_no = None if full_refresh else services._max_cached_post_no(db, thread_url)
    effective_full_refresh = full_refresh or stop_at_post_no is None

    result = services.fetch_posts_from_thread(
        thread_url,
        stop_at_post_no=stop_at_post_no,
    )
    trace = list(getattr(result, "trace", []) or [])
    post_list = list(result)

    if effective_full_refresh and not any(
        getattr(post, "post_no", None) == 1 for post in post_list
    ):
        numbers = [
            int(post.post_no)
            for post in post_list
            if getattr(post, "post_no", None) is not None
        ]
        _LOGGER.warning(
            "[THREAD_REFRESH][incomplete] url=%s count=%s min_no=%s max_no=%s "
            "visited=%s trace=%s",
            thread_url,
            len(post_list),
            min(numbers) if numbers else None,
            max(numbers) if numbers else None,
            len(trace),
            " | ".join(trace) if trace else "-",
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
    """HTML内の通常ページャー巡回と不完全更新防止を起動時に有効化する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    scraper.fetch_posts_from_thread = _crawl_thread_pages
    services.fetch_posts_from_thread = _crawl_thread_pages
    services._refresh_cached_thread = _guarded_refresh_cached_thread

    _INSTALLED = True
