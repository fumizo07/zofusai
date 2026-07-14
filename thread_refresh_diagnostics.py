# thread_refresh_diagnostics.py
"""爆サイスレッド全件取得の一時診断と不完全更新防止。"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Iterable, Optional
from urllib.parse import urljoin

import scraper
import services


logger = logging.getLogger("uvicorn.error")
_INSTALLED = False
_PREFERRED_FETCH_MODE: Optional[str] = None


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


def _post_signature(posts: Iterable[object]) -> tuple:
    numbered: list[int] = []
    fallback: list[tuple[str, str]] = []
    for post in posts:
        value = getattr(post, "post_no", None)
        if value is not None:
            try:
                numbered.append(int(value))
                continue
            except (TypeError, ValueError):
                pass
        fallback.append(
            (
                str(getattr(post, "posted_at", None) or ""),
                str(getattr(post, "body", None) or ""),
            )
        )
    if numbered:
        return ("numbered", tuple(sorted(numbered)))
    return ("unknown", tuple(sorted(fallback)))


def _browser_headers(headers: dict, url: str) -> dict:
    out = dict(headers or {})
    out.update(
        {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "Referer": scraper._strip_page_segment(url),
        }
    )
    return out


def _short_url_list(values: Iterable[str], response_url: str) -> str:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        absolute = urljoin(response_url, (value or "").strip())
        if not absolute or absolute in seen:
            continue
        if "/p=" not in absolute:
            continue
        seen.add(absolute)
        unique.append(absolute)
        if len(unique) >= 8:
            break
    return "|".join(unique) if unique else "-"


def _probe_page(url: str, headers: dict, mode: str):
    probe_session = scraper.requests.Session()
    request_headers = dict(headers or {})
    if mode == "browser":
        request_headers = _browser_headers(request_headers, url)

    started = time.monotonic()
    response = probe_session.get(url, headers=request_headers, timeout=10)
    response.raise_for_status()

    html = response.text or ""
    soup = scraper.BeautifulSoup(html, "html.parser")
    posts = scraper._parse_posts_from_soup(soup)
    response_url = getattr(response, "url", "") or url
    html_thread_url = scraper._extract_ttgid_base_url(soup, url, response_url)
    resolved_url = html_thread_url or response_url

    canonical_node = soup.select_one("link[rel='canonical'][href]")
    canonical = (canonical_node.get("href") or "").strip() if canonical_node else "-"
    og_node = soup.select_one("meta[property='og:url'][content]")
    og_url = (og_node.get("content") or "").strip() if og_node else "-"

    pager_links = _short_url_list(
        (node.get("href") or "" for node in soup.select("a[href*='/p='], a[href*='p=']")),
        response_url,
    )
    cookie_names = ",".join(sorted(probe_session.cookies.keys())) or "-"
    history = ">".join(str(item.status_code) for item in response.history) or "-"

    min_no, max_no, count = _number_range(posts)
    logger.info(
        "[THREAD_DIAG][probe] mode=%s request_url=%s final_url=%s "
        "count=%s min_no=%s max_no=%s status=%s history=%s html_len=%s html_sha=%s "
        "canonical=%s og_url=%s res_dl=%s res_ul=%s res_div=%s res_article=%s "
        "cookie_names=%s pager_links=%s elapsed=%.2f",
        mode,
        url,
        response_url,
        count,
        min_no,
        max_no,
        response.status_code,
        history,
        len(html),
        hashlib.sha256(html.encode("utf-8", errors="replace")).hexdigest()[:16],
        canonical,
        og_url,
        len(soup.select("dl#res_list div.article.res_list_article")),
        len(soup.select("ul#res_list li.res_block")),
        len(soup.select("#res_list div.res_list_article")),
        len(soup.select("#res_list article.res_list_article")),
        cookie_names,
        pager_links,
        time.monotonic() - started,
    )

    return scraper.PageFetchResult(posts=posts, final_url=resolved_url)


def install_thread_refresh_diagnostics() -> None:
    """一時診断を有効化し、不完全な全件取得を成功扱いしない。"""
    global _INSTALLED
    if _INSTALLED:
        return

    original_fetch_single_page = scraper._fetch_single_page
    original_fetch_posts = scraper.fetch_posts_from_thread

    def traced_fetch_single_page(session, url: str, headers: dict):
        global _PREFERRED_FETCH_MODE

        if _PREFERRED_FETCH_MODE is not None:
            try:
                result = _probe_page(url, headers, _PREFERRED_FETCH_MODE)
                min_no, max_no, count = _number_range(result.posts)
                logger.info(
                    "[THREAD_DIAG][page_override] mode=%s request_url=%s final_url=%s "
                    "count=%s min_no=%s max_no=%s",
                    _PREFERRED_FETCH_MODE,
                    url,
                    result.final_url,
                    count,
                    min_no,
                    max_no,
                )
                return result
            except Exception as exc:
                logger.warning(
                    "[THREAD_DIAG][override_error] mode=%s request_url=%s error=%s",
                    _PREFERRED_FETCH_MODE,
                    url,
                    exc,
                )

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

        shared_signature = _post_signature(result.posts)
        if "ttgid=" in url:
            for mode in ("clean", "browser"):
                try:
                    probe_result = _probe_page(url, headers, mode)
                except Exception as exc:
                    logger.warning(
                        "[THREAD_DIAG][probe_error] mode=%s request_url=%s error=%s",
                        mode,
                        url,
                        exc,
                    )
                    continue

                if probe_result.posts and _post_signature(probe_result.posts) != shared_signature:
                    _PREFERRED_FETCH_MODE = mode
                    pmin, pmax, pcount = _number_range(probe_result.posts)
                    logger.warning(
                        "[THREAD_DIAG][session_or_header_cause] mode=%s request_url=%s "
                        "shared_min=%s shared_max=%s probe_min=%s probe_max=%s probe_count=%s "
                        "action=use_probe_result",
                        mode,
                        url,
                        min_no,
                        max_no,
                        pmin,
                        pmax,
                        pcount,
                    )
                    return probe_result

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
