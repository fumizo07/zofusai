# thread_refresh_diagnostics.py
"""爆サイのページ取得内容を特定するための一時診断ログ。"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Optional

import scraper
import thread_refresh_browser as browser_refresh
import thread_refresh_fix as refresh_fix


_LOGGER = logging.getLogger(__name__)
_INSTALLED = False
_SAFE_HEADER_NAMES = (
    "server",
    "content-type",
    "cache-control",
    "age",
    "x-cache",
    "cf-cache-status",
)
_BLOCK_PHRASES = (
    "アクセスが集中",
    "アクセス制限",
    "しばらく時間をおいて",
    "不正なアクセス",
    "captcha",
    "ロボットではありません",
    "JavaScriptを有効",
    "ページが見つかりません",
)


def _compact(value: object, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text or "-"


def _html_fingerprint(html: str) -> str:
    return hashlib.sha256((html or "").encode("utf-8", errors="replace")).hexdigest()[:16]


def _title(soup) -> str:
    try:
        return _compact(soup.title.get_text(" ", strip=True) if soup.title else "-")
    except Exception:
        return "-"


def _metadata_url(soup, selector: str, attr: str) -> str:
    try:
        node = soup.select_one(selector)
        return _compact(node.get(attr) if node is not None else "-")
    except Exception:
        return "-"


def _pager_numbers(soup) -> str:
    numbers: set[int] = set()
    try:
        for node in soup.select("a[href], option[value], form[action]"):
            for attr in ("href", "value", "action"):
                value = (node.get(attr) or "").strip()
                if not value:
                    continue
                page_no = refresh_fix._page_number(value)
                if page_no < 10**9:
                    numbers.add(page_no)
                break
    except Exception:
        return "-"
    return ",".join(str(number) for number in sorted(numbers)) or "-"


def _block_flags(html: str) -> str:
    lowered = (html or "").lower()
    found = [phrase for phrase in _BLOCK_PHRASES if phrase.lower() in lowered]
    return ",".join(found) or "-"


def _header_summary(headers) -> str:
    if not headers:
        return "-"
    items = []
    for name in _SAFE_HEADER_NAMES:
        try:
            value = headers.get(name)
        except Exception:
            value = None
        if value:
            items.append(f"{name}={_compact(value, 80)}")
    return ";".join(items) or "-"


def _cookie_names_from_session(session) -> str:
    try:
        names = sorted({cookie.name for cookie in session.cookies})
    except Exception:
        names = []
    return ",".join(names) or "-"


def _cookie_names_from_context(page) -> str:
    try:
        names = sorted({item.get("name", "") for item in page.context.cookies() if item.get("name")})
    except Exception:
        names = []
    return ",".join(names) or "-"


def _html_diagnostics(soup, html: str) -> str:
    return (
        f"html_len={len(html or '')} html_sha={_html_fingerprint(html)} "
        f"title={_title(soup)} "
        f"canonical={_metadata_url(soup, 'link[rel=\"canonical\"][href]', 'href')} "
        f"og_url={_metadata_url(soup, 'meta[property=\"og:url\"][content]', 'content')} "
        f"resbody={len(soup.select('div.resbody'))} "
        f"comment_text={len(soup.select('[itemprop=\"commentText\"]'))} "
        f"dd_body={len(soup.select('dd.body'))} "
        f"res_list={len(soup.select('#res_list'))} "
        f"pager={_pager_numbers(soup)} block_flags={_block_flags(html)}"
    )


def _fetch_page_diagnostic(
    session,
    url: str,
    headers: dict,
    *,
    referer: Optional[str] = None,
):
    """requests取得のHTML状態を記録し、既存処理と同じ結果を返す。"""
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            request_headers = dict(headers or {})
            if referer:
                request_headers["Referer"] = referer

            response = session.get(url, headers=request_headers, timeout=10)
            html = response.text or ""
            soup = scraper.BeautifulSoup(html, "html.parser")
            posts = scraper._parse_posts_from_soup(soup)
            min_no, max_no, count = refresh_fix._number_range(posts)
            final_url = getattr(response, "url", "") or url

            _LOGGER.warning(
                "[THREAD_DIAG][requests_html] url=%s attempt=%s final_url=%s "
                "referer=%s status=%s count=%s range=%s-%s cookies=%s headers=%s %s",
                url,
                attempt + 1,
                final_url,
                referer,
                response.status_code,
                count,
                min_no,
                max_no,
                _cookie_names_from_session(session),
                _header_summary(getattr(response, "headers", None)),
                _html_diagnostics(soup, html),
            )

            if response.status_code != 200:
                raise scraper.ScrapingError(
                    f"HTTPステータスコードが異常です: {response.status_code}"
                )

            if posts:
                links = refresh_fix._extract_pager_links(soup, url, final_url)
                return posts, final_url, links

            last_error = scraper.ScrapingError(
                "投稿らしきテキストが見つかりませんでした。"
            )
        except Exception as exc:
            last_error = exc

        if attempt < 2:
            time.sleep(0.6 * (attempt + 1))

    if isinstance(last_error, scraper.ScrapingError):
        raise last_error
    raise scraper.ScrapingError(f"ページ取得に失敗しました: {last_error}")


def _navigate_and_parse_diagnostic(page, target_url: str, source_url: Optional[str]):
    """Playwright遷移後のDOM状態を記録し、既存処理と同じ結果を返す。"""
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
    min_no, max_no, count = refresh_fix._number_range(posts)
    status = getattr(response, "status", None) if response is not None else None
    response_headers = None
    if response is not None:
        try:
            response_headers = response.all_headers()
        except Exception:
            response_headers = None

    _LOGGER.warning(
        "[THREAD_DIAG][browser_html] target_url=%s final_url=%s source_url=%s "
        "status=%s count=%s range=%s-%s cookies=%s headers=%s nav_error=%s %s",
        target_url,
        final_url,
        source_url,
        status,
        count,
        min_no,
        max_no,
        _cookie_names_from_context(page),
        _header_summary(response_headers),
        browser_refresh._error_text(navigation_error) if navigation_error else "-",
        _html_diagnostics(soup, html),
    )

    if not posts:
        if navigation_error is not None:
            raise scraper.ScrapingError(
                "ページ遷移後も投稿を取得できませんでした: "
                f"{browser_refresh._error_text(navigation_error)}"
            ) from navigation_error
        raise scraper.ScrapingError(
            f"ブラウザページから投稿を取得できませんでした。status={status}"
        )

    links = refresh_fix._extract_pager_links(soup, target_url, final_url)
    return posts, final_url, links, status, navigation_error


def _parse_fetched_html_diagnostic(result: dict, target_url: str):
    """ブラウザ内fetchの生HTML状態を記録する。"""
    error = (result.get("error") or "").strip()
    html = result.get("html") or ""
    final_url = result.get("finalUrl") or target_url
    status = result.get("status")
    soup = scraper.BeautifulSoup(html, "html.parser")
    posts = scraper._parse_posts_from_soup(soup)
    min_no, max_no, count = refresh_fix._number_range(posts)

    _LOGGER.warning(
        "[THREAD_DIAG][browser_fetch_html] target_url=%s final_url=%s status=%s "
        "count=%s range=%s-%s fetch_error=%s %s",
        target_url,
        final_url,
        status,
        count,
        min_no,
        max_no,
        _compact(error),
        _html_diagnostics(soup, html),
    )

    if error:
        raise scraper.ScrapingError(f"browser fetch failed: {error}")
    if not posts:
        raise scraper.ScrapingError(
            f"browser fetchで投稿を取得できませんでした。status={status}"
        )

    links = refresh_fix._extract_pager_links(soup, target_url, final_url)
    return posts, final_url, links, status, None


def install_thread_refresh_diagnostics() -> None:
    """一時診断ログをrequests・Playwright取得へ組み込む。"""
    global _INSTALLED
    if _INSTALLED:
        return

    refresh_fix._fetch_page = _fetch_page_diagnostic
    browser_refresh._navigate_and_parse = _navigate_and_parse_diagnostic
    browser_refresh._parse_fetched_html = _parse_fetched_html_diagnostic
    _INSTALLED = True
