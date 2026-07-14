import logging
import time
from functools import wraps

import scraper


_INSTALLED = False


def _post_number_range(posts):
    numbers = [post.post_no for post in posts if getattr(post, "post_no", None) is not None]
    if not numbers:
        return None, None
    return min(numbers), max(numbers)


def install_scraper_diagnostics() -> None:
    """爆サイのページ巡回状況をRenderログへ一時的に記録する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch_single_page = scraper._fetch_single_page

    @wraps(original_fetch_single_page)
    def logged_fetch_single_page(session, url, headers):
        started = time.monotonic()
        try:
            result = original_fetch_single_page(session, url, headers)
        except Exception as exc:
            logging.exception(
                "[SCRAPER_DIAG][page_error] request_url=%s elapsed=%.2f error=%s",
                url,
                time.monotonic() - started,
                exc,
            )
            raise

        min_no, max_no = _post_number_range(result.posts)
        logging.info(
            "[SCRAPER_DIAG][page] request_url=%s final_url=%s count=%d "
            "min_no=%s max_no=%s elapsed=%.2f",
            url,
            result.final_url,
            len(result.posts),
            min_no,
            max_no,
            time.monotonic() - started,
        )
        return result

    scraper._fetch_single_page = logged_fetch_single_page

    original_fetch_posts = scraper.fetch_posts_from_thread

    @wraps(original_fetch_posts)
    def logged_fetch_posts(url, max_pages=20, stop_at_post_no=None):
        started = time.monotonic()
        logging.info(
            "[SCRAPER_DIAG][start] url=%s max_pages=%s stop_at_post_no=%s",
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
            logging.exception(
                "[SCRAPER_DIAG][fetch_error] url=%s elapsed=%.2f error=%s",
                url,
                time.monotonic() - started,
                exc,
            )
            raise

        min_no, max_no = _post_number_range(posts)
        logging.info(
            "[SCRAPER_DIAG][finish] url=%s count=%d min_no=%s max_no=%s "
            "reached_oldest=%s elapsed=%.2f",
            url,
            len(posts),
            min_no,
            max_no,
            min_no == 1,
            time.monotonic() - started,
        )
        return posts

    scraper.fetch_posts_from_thread = logged_fetch_posts

    # services.pyは関数を直接importしているため、参照先も診断ラッパーへ切り替える。
    import services

    services.fetch_posts_from_thread = logged_fetch_posts
    _INSTALLED = True
    logging.info("[SCRAPER_DIAG][installed]")
