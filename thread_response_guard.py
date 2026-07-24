# thread_response_guard.py
"""爆サイから返されたHTMLが要求したスレッドか検証する。"""

from __future__ import annotations

import logging
import random
import re
import time
from typing import Optional

import scraper
import thread_refresh_browser as browser_refresh
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False
_POST_SELECTOR = "div.resbody, [itemprop='commentText'], dd.body"
_BROWSER_POST_WAIT_MS = 5_000


class _ThreadPageMismatch(scraper.ScrapingError):
    """要求したスレッドと取得HTMLが一致しない。"""


def _document_thread_id(soup) -> Optional[str]:
    """canonical・OG URL・NO.表示から現在ページのスレッドIDを取得する。"""
    for selector, attr in (
        ("link[rel='canonical'][href]", "href"),
        ("meta[property='og:url'][content]", "content"),
    ):
        for node in soup.select(selector):
            thread_id = refresh_fix._thread_id((node.get(attr) or "").strip())
            if thread_id:
                return thread_id

    marker = soup.find(string=re.compile(r"\bNO\.\s*\d+\b", re.IGNORECASE))
    if marker:
        match = re.search(r"\bNO\.\s*(\d+)\b", str(marker), re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _declared_response_count(soup) -> Optional[int]:
    """ページタイトルの「693レス」などからスレッド総レス数を取得する。"""
    try:
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
    except Exception:
        title = ""

    match = re.search(r"(?:｜|\|)\s*([\d,]+)\s*レス(?:｜|\|)", title)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _page_diagnostics(soup, html: str) -> str:
    try:
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
    except Exception:
        title = ""
    title = " ".join(title.split())[:160] or "-"
    return (
        f"title={title} html_tid={_document_thread_id(soup) or '-'} "
        f"declared={_declared_response_count(soup)} len={len(html or '')}"
    )


def _validate_thread_page(soup, target_url: str, final_url: str, posts) -> None:
    """URL、HTML内ID、レス番号が同じスレッドとして矛盾しないか確認する。"""
    requested_tid = refresh_fix._thread_id(target_url)
    if not requested_tid:
        raise _ThreadPageMismatch("要求URLからスレッドIDを取得できませんでした。")

    final_tid = refresh_fix._thread_id(final_url)
    if final_tid and final_tid != requested_tid:
        raise _ThreadPageMismatch(
            "最終URLのスレッドIDが一致しません: "
            f"requested={requested_tid} final={final_tid}"
        )

    document_tid = _document_thread_id(soup)
    if document_tid and document_tid != requested_tid:
        raise _ThreadPageMismatch(
            "取得HTMLのスレッドIDが一致しません: "
            f"requested={requested_tid} html={document_tid}"
        )

    declared_count = _declared_response_count(soup)
    numbers = [
        int(post.post_no)
        for post in posts
        if getattr(post, "post_no", None) is not None
    ]
    if declared_count is not None and numbers and max(numbers) > declared_count:
        raise _ThreadPageMismatch(
            "取得レス番号がHTML記載の総レス数を超えています: "
            f"requested={requested_tid} max={max(numbers)} declared={declared_count}"
        )


def _fetch_page_guarded(
    session,
    url: str,
    headers: dict,
    *,
    referer: Optional[str] = None,
):
    """requests取得時に対象スレッドのHTMLか検証してから投稿を採用する。"""
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            request_headers = dict(headers or {})
            request_headers["Cache-Control"] = "no-cache"
            request_headers["Pragma"] = "no-cache"
            if referer:
                request_headers["Referer"] = referer

            response = session.get(url, headers=request_headers, timeout=10)
            if response.status_code != 200:
                raise scraper.ScrapingError(
                    f"HTTPステータスコードが異常です: {response.status_code}"
                )

            html = response.text or ""
            soup = scraper.BeautifulSoup(html, "html.parser")
            posts = scraper._parse_posts_from_soup(soup)
            final_url = getattr(response, "url", "") or url

            if not posts:
                raise scraper.ScrapingError(
                    "投稿らしきテキストが見つかりませんでした。 "
                    + _page_diagnostics(soup, html)
                )

            _validate_thread_page(soup, url, final_url, posts)
            links = refresh_fix._extract_pager_links(soup, url, final_url)
            return posts, final_url, links
        except Exception as exc:
            last_error = exc
            if isinstance(exc, _ThreadPageMismatch):
                _LOGGER.warning(
                    "[THREAD_GUARD][requests_mismatch] url=%s attempt=%s error=%s",
                    url,
                    attempt + 1,
                    exc,
                )
                try:
                    session.cookies.clear()
                except Exception:
                    pass

        if attempt < 2:
            time.sleep(0.6 * (attempt + 1) + random.uniform(0.0, 0.4))

    if isinstance(last_error, scraper.ScrapingError):
        raise last_error
    raise scraper.ScrapingError(f"ページ取得に失敗しました: {last_error}")


def _wait_for_posts(page) -> None:
    try:
        page.wait_for_selector(
            _POST_SELECTOR,
            state="attached",
            timeout=_BROWSER_POST_WAIT_MS,
        )
    except Exception:
        pass


def _navigate_and_parse_guarded(page, target_url: str, source_url: Optional[str]):
    """Playwrightで投稿の表示を待ち、対象スレッドか検証してから返す。"""
    last_error: Optional[Exception] = None

    try:
        page.set_extra_http_headers(
            {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        )
    except Exception:
        pass

    for attempt in range(2):
        response = None
        navigation_error: Optional[Exception] = None
        try:
            goto_options = {"wait_until": "domcontentloaded"}
            if source_url:
                goto_options["referer"] = source_url
            response = page.goto(target_url, **goto_options)
        except Exception as exc:
            navigation_error = exc

        _wait_for_posts(page)
        final_url = page.url or target_url
        html = page.content() or ""
        soup = scraper.BeautifulSoup(html, "html.parser")
        posts = scraper._parse_posts_from_soup(soup)
        status = getattr(response, "status", None) if response is not None else None

        try:
            if not posts:
                if navigation_error is not None:
                    raise scraper.ScrapingError(
                        "ページ遷移後も投稿を取得できませんでした: "
                        f"{browser_refresh._error_text(navigation_error)} "
                        f"status={status} {_page_diagnostics(soup, html)}"
                    ) from navigation_error
                raise scraper.ScrapingError(
                    "ブラウザページから投稿を取得できませんでした。"
                    f"status={status} {_page_diagnostics(soup, html)}"
                )

            _validate_thread_page(soup, target_url, final_url, posts)
            links = refresh_fix._extract_pager_links(soup, target_url, final_url)
            return posts, final_url, links, status, navigation_error
        except Exception as exc:
            last_error = exc
            if isinstance(exc, _ThreadPageMismatch):
                _LOGGER.warning(
                    "[THREAD_GUARD][browser_mismatch] url=%s attempt=%s error=%s",
                    target_url,
                    attempt + 1,
                    exc,
                )
                try:
                    page.context.clear_cookies()
                except Exception:
                    pass

        if attempt == 0:
            try:
                page.wait_for_timeout(800)
            except Exception:
                pass

    if isinstance(last_error, scraper.ScrapingError):
        raise last_error
    raise scraper.ScrapingError(f"ブラウザページ取得に失敗しました: {last_error}")


def _parse_fetched_html_guarded(result: dict, target_url: str):
    """ブラウザ内fetchのHTMLも対象スレッドか検証する。"""
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
            "browser fetchで投稿を取得できませんでした。"
            f"status={status} {_page_diagnostics(soup, html)}"
        )

    _validate_thread_page(soup, target_url, final_url, posts)
    links = refresh_fix._extract_pager_links(soup, target_url, final_url)
    return posts, final_url, links, status, None


def install_thread_response_guard() -> None:
    """requestsとPlaywrightの取得HTML検証を起動時に有効化する。"""
    global _INSTALLED
    if _INSTALLED:
        return

    refresh_fix._fetch_page = _fetch_page_guarded
    browser_refresh._navigate_and_parse = _navigate_and_parse_guarded
    browser_refresh._parse_fetched_html = _parse_fetched_html_guarded
    _INSTALLED = True
