"""最新レス印を使い、爆サイスレッドの増分確認と期限切れ確認を高速化する。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Optional

import scraper
import services
import thread_refresh_browser as browser_refresh
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False


@dataclass
class _LatestSnapshot:
    marker_text: str
    marker_no: int
    posts: list[object]
    final_url: str


def _is_hidden(node) -> bool:
    """display:none等で非表示になっているダミー最新レス印を除外する。"""
    current = node
    while current is not None and getattr(current, "name", None) not in ("html", "body"):
        style = (current.get("style") or "").replace(" ", "").lower()
        if "display:none" in style:
            return True
        if current.has_attr("hidden"):
            return True
        if (current.get("aria-hidden") or "").strip().lower() == "true":
            return True
        current = current.parent
    return False


def _response_container(node):
    for parent in node.parents:
        if getattr(parent, "name", None) in ("html", "body"):
            break
        if scraper._looks_like_response_container(parent):
            return parent
    return None


def _extract_visible_latest_marker(soup) -> Optional[tuple[str, int]]:
    """
    表示中の「最新レス」「最終レス」と、そのレス番号を返す。

    現行スレッドにある非表示の#0ダミーは、display:noneとレス番号0の
    両方で除外する。
    """
    candidates: list[tuple[int, str]] = []

    for marker in soup.select("span.latestCmnt"):
        marker_text = marker.get_text(" ", strip=True)
        if marker_text not in ("最新レス", "最終レス"):
            continue
        if _is_hidden(marker):
            continue

        container = _response_container(marker)
        if container is None:
            continue

        post_no = scraper._extract_post_no(container)
        if post_no is None or int(post_no) <= 0:
            continue
        candidates.append((int(post_no), marker_text))

    if not candidates:
        return None

    post_no, marker_text = max(candidates, key=lambda item: item[0])
    return marker_text, post_no


def _fetch_latest_snapshot(url: str) -> Optional[_LatestSnapshot]:
    """ルートページを1回取得し、表示中の最新・最終レスと投稿一覧を読む。"""
    root_url = refresh_fix._thread_root(url) or url
    try:
        response = scraper.requests.get(
            root_url,
            headers=scraper._build_headers(),
            timeout=10,
        )
    except Exception as exc:
        _LOGGER.info(
            "[THREAD_INCREMENTAL][snapshot_failed] url=%s error=%s",
            url,
            browser_refresh._error_text(exc),
        )
        return None

    if response.status_code != 200:
        _LOGGER.info(
            "[THREAD_INCREMENTAL][snapshot_failed] url=%s status=%s",
            url,
            response.status_code,
        )
        return None

    soup = scraper.BeautifulSoup(response.text or "", "html.parser")
    marker = _extract_visible_latest_marker(soup)
    if marker is None:
        _LOGGER.info(
            "[THREAD_INCREMENTAL][snapshot_fallback] url=%s reason=latest_marker_missing",
            url,
        )
        return None

    posts = list(scraper._parse_posts_from_soup(soup))
    if not posts:
        _LOGGER.info(
            "[THREAD_INCREMENTAL][snapshot_fallback] url=%s reason=posts_missing",
            url,
        )
        return None

    marker_text, marker_no = marker
    return _LatestSnapshot(
        marker_text=marker_text,
        marker_no=marker_no,
        posts=posts,
        final_url=getattr(response, "url", "") or root_url,
    )


def _numbered_posts(posts: Iterable[object]) -> dict[int, object]:
    result: dict[int, object] = {}
    for post in posts:
        post_no = getattr(post, "post_no", None)
        if post_no is None:
            continue
        result.setdefault(int(post_no), post)
    return result


def _complete_new_range(
    snapshot: _LatestSnapshot,
    stop_at_post_no: int,
) -> Optional[list[object]]:
    """ルートページだけで、キャッシュ以降の全レスが揃っている場合に返す。"""
    numbered = _numbered_posts(snapshot.posts)
    if snapshot.marker_no < stop_at_post_no:
        return None
    if snapshot.marker_no == stop_at_post_no:
        return []

    expected = set(range(stop_at_post_no + 1, snapshot.marker_no + 1))
    if not expected.issubset(numbered):
        return None

    return [numbered[post_no] for post_no in sorted(expected)]


def _contiguous_cache_max(db, thread_url: str) -> Optional[int]:
    """キャッシュが#1から最大番号まで連続している場合だけ最大番号を返す。"""
    min_no, max_no, distinct_count = (
        db.query(
            services.func.min(services.CachedPost.post_no),
            services.func.max(services.CachedPost.post_no),
            services.func.count(
                services.func.distinct(services.CachedPost.post_no)
            ),
        )
        .filter(
            services.CachedPost.thread_url == thread_url,
            services.CachedPost.post_no.isnot(None),
        )
        .one()
    )

    if min_no is None or max_no is None:
        return None

    min_no = int(min_no)
    max_no = int(max_no)
    distinct_count = int(distinct_count or 0)
    if min_no != 1 or distinct_count != max_no:
        return None
    return max_no


def install_incremental_thread_fastpath() -> None:
    """最新レス印を使った1ページ確認を、通常の全ページ巡回より先に試す。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch = services.fetch_posts_from_thread
    original_refresh = services._refresh_cached_thread

    def fetch_posts_incremental_fast(
        url: str,
        max_pages: int = 20,
        stop_at_post_no: Optional[int] = None,
    ):
        if stop_at_post_no is None:
            return original_fetch(
                url,
                max_pages=max_pages,
                stop_at_post_no=stop_at_post_no,
            )

        snapshot = _fetch_latest_snapshot(url)
        if snapshot is not None:
            new_posts = _complete_new_range(snapshot, int(stop_at_post_no))
            if new_posts is not None:
                action = "unchanged" if not new_posts else "updated"
                _LOGGER.info(
                    "[THREAD_INCREMENTAL][%s] url=%s marker=%s marker_no=%s "
                    "cached_max=%s new_count=%s",
                    action,
                    url,
                    snapshot.marker_text,
                    snapshot.marker_no,
                    stop_at_post_no,
                    len(new_posts),
                )
                return refresh_fix.CrawlPosts(
                    new_posts,
                    trace=[
                        f"{refresh_fix._trace_path(snapshot.final_url)} "
                        f"mode=latest_marker marker={snapshot.marker_text} "
                        f"marker_no={snapshot.marker_no} "
                        f"cached_max={stop_at_post_no} new={len(new_posts)}"
                    ],
                )

        return original_fetch(
            url,
            max_pages=max_pages,
            stop_at_post_no=stop_at_post_no,
        )

    def refresh_cached_thread_fast(
        db,
        thread_url: str,
        *,
        full_refresh: bool,
    ) -> None:
        if not full_refresh:
            return original_refresh(
                db,
                thread_url,
                full_refresh=full_refresh,
            )

        try:
            cached_max = _contiguous_cache_max(db, thread_url)
            if cached_max is not None:
                snapshot = _fetch_latest_snapshot(thread_url)
                if snapshot is not None:
                    new_posts = _complete_new_range(snapshot, cached_max)
                    if new_posts is not None:
                        services._save_thread_posts_to_cache(
                            db,
                            thread_url,
                            new_posts,
                            full_refresh=True,
                        )
                        action = "full_verified" if not new_posts else "full_extended"
                        _LOGGER.info(
                            "[THREAD_INCREMENTAL][%s] url=%s marker=%s "
                            "marker_no=%s cached_max=%s new_count=%s",
                            action,
                            thread_url,
                            snapshot.marker_text,
                            snapshot.marker_no,
                            cached_max,
                            len(new_posts),
                        )
                        return
        except Exception as exc:
            db.rollback()
            _LOGGER.warning(
                "[THREAD_INCREMENTAL][fast_check_failed] url=%s error=%s",
                thread_url,
                browser_refresh._error_text(exc),
            )

        return original_refresh(
            db,
            thread_url,
            full_refresh=full_refresh,
        )

    scraper.fetch_posts_from_thread = fetch_posts_incremental_fast
    services.fetch_posts_from_thread = fetch_posts_incremental_fast
    services._refresh_cached_thread = refresh_cached_thread_fast
    _INSTALLED = True
