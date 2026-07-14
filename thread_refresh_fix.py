# thread_refresh_fix.py
"""爆サイのページ指定を現行のURL形式へ合わせる恒久修正。"""

from __future__ import annotations

import re

import scraper
import services


_INSTALLED = False


def _strip_paging_segments(url: str) -> str:
    """p・tp・ttgidを除き、スレッドの基準URLへ統一する。"""
    raw = (url or "").strip()
    if not raw:
        return ""

    scheme_marker = "__SCHEME_SLASHES__"
    base = raw.split("#", 1)[0].split("?", 1)[0]
    base = base.replace("://", scheme_marker)
    base = re.sub(r"/(?:p|tp|ttgid)=\d+", "", base)
    base = re.sub(r"/{2,}", "/", base)
    base = base.replace(scheme_marker, "://")
    if not base.endswith("/"):
        base += "/"
    return base


def _make_page_url(base_url: str, page: int) -> str:
    """爆サイの正式なページ指定であるp=数字/tp=1を使用する。"""
    base = _strip_paging_segments(base_url)
    page_no = max(1, int(page))
    return f"{base}p={page_no}/tp=1/"


def _guarded_refresh_cached_thread(
    db,
    thread_url: str,
    *,
    full_refresh: bool,
) -> None:
    """#1未到達の全件取得を成功扱いせず、古いキャッシュを維持する。"""
    stop_at_post_no = None if full_refresh else services._max_cached_post_no(db, thread_url)
    effective_full_refresh = full_refresh or stop_at_post_no is None

    posts = services.fetch_posts_from_thread(
        thread_url,
        stop_at_post_no=stop_at_post_no,
    )
    post_list = list(posts)

    if effective_full_refresh and not any(
        getattr(post, "post_no", None) == 1 for post in post_list
    ):
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
    """現行ページ指定と不完全更新防止を起動時に有効化する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    scraper._strip_page_segment = _strip_paging_segments
    scraper.make_page_url = _make_page_url
    services._refresh_cached_thread = _guarded_refresh_cached_thread

    _INSTALLED = True
