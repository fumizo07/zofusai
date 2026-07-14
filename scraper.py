# 001
# scraper.py
import logging
import random
import re
import time

from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup


class ScrapingError(Exception):
    """スクレイピング時のエラー用のカスタム例外"""
    pass


@dataclass
class ScrapedPost:
    post_no: Optional[int]
    posted_at: Optional[str]
    body: str
    anchors: List[int]


anchor_pattern = re.compile(r">>(\d+)")

# Renderのプロセス起動後、各スレッドで一度だけ全ページ補修を行う。
# 再起動後に再度補修されてもUPSERTなので既存キャッシュは壊れない。
_REPAIRED_THREAD_URLS: set[str] = set()


def extract_anchors(text: str) -> List[int]:
    """本文中の >>123 のようなアンカーを整数リストで返す。"""
    nums = [int(match.group(1)) for match in anchor_pattern.finditer(text)]
    return sorted(set(nums))


def parse_int_from_text(text: str) -> Optional[int]:
    """'#55' や 'res55_block' のような文字列から整数を取り出す。"""
    match = re.search(r"(\d+)", text or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _strip_page_segment(url: str) -> str:
    """スレURLから /p=数字/ を除き、ページ指定なしURLへ統一する。"""
    raw = (url or "").strip()
    if not raw:
        return ""

    scheme_marker = "__SCHEME_SLASHES__"
    base = raw.replace("://", scheme_marker)
    base = re.sub(r"/p=\d+/?", "/", base)
    base = re.sub(r"/{2,}", "/", base)
    base = base.replace(scheme_marker, "://")
    if not base.endswith("/"):
        base += "/"
    return base


def make_page_url(base_url: str, page: int) -> str:
    """
    ページ指定なしURLとp=1は最新側を表示する。
    p=2以降は数字が増えるほど古い側へ進む。
    """
    base = _strip_page_segment(base_url)
    page_no = max(1, int(page))
    if page_no == 1:
        return base
    return f"{base}p={page_no}/"


def _build_headers() -> dict:
    """リクエストヘッダを組み立てる。"""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }


def get_thread_title(url: str) -> Optional[str]:
    """スレッドページのタイトル文字列を取得する。"""
    try:
        response = requests.get(url, headers=_build_headers(), timeout=10)
    except Exception:
        return None

    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    if soup.title and soup.title.string:
        return soup.title.string.strip()

    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
        if title:
            return title
    return None


def _parse_post_no_candidate(value: Optional[str]) -> Optional[int]:
    """レス番号候補から、無関係な数字を避けながら番号を取り出す。"""
    value = (value or "").strip()
    if not value:
        return None

    patterns = (
        r"(?:^|[#_\-/])(?:res|rrid|no)?[=_-]?(\d+)(?:$|[_\-/])",
        r"(?:rrid|resno|res_no|post_no|data-no)[=/=_-]?(\d+)",
        r"^#?\s*(\d+)\s*$",
    )
    for pattern in patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def _extract_post_no(element) -> Optional[int]:
    """HTML表現の揺れを考慮してレス番号を取得する。"""
    selectors = (
        "span.resnumb a",
        "span.resnumb",
        "a.resnumb",
        "[data-res-no]",
        "[data-resno]",
        "[data-post-no]",
        "[data-no]",
    )

    for selector in selectors:
        node = element.select_one(selector)
        if not node:
            continue

        for attr in ("data-res-no", "data-resno", "data-post-no", "data-no", "id", "href"):
            post_no = _parse_post_no_candidate(node.get(attr))
            if post_no is not None:
                return post_no

        post_no = _parse_post_no_candidate(node.get_text(" ", strip=True))
        if post_no is not None:
            return post_no

    for attr in ("data-res-no", "data-resno", "data-post-no", "data-no", "id"):
        post_no = _parse_post_no_candidate(element.get(attr))
        if post_no is not None:
            return post_no

    for link in element.select("a[href]"):
        href = link.get("href") or ""
        match = re.search(r"(?:^|/)rrid=(\d+)(?:/|$)", href)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
    return None


def _looks_like_response_container(tag) -> bool:
    if not getattr(tag, "name", None):
        return False
    if tag.name in ("article", "li"):
        return True

    tag_id = (tag.get("id") or "").lower()
    classes = " ".join(tag.get("class") or []).lower()
    marker = f"{tag_id} {classes}"
    return any(word in marker for word in ("res_block", "res_list_article", "article", "response"))


def _select_response_elements(soup: BeautifulSoup):
    """既知セレクタに加え、本文要素から親レス要素を逆引きする。"""
    selectors = (
        "dl#res_list div.article.res_list_article",
        "ul#res_list li.res_block",
        "#res_list div.res_list_article",
        "#res_list article.res_list_article",
        "#res_list li.res_block",
    )
    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            return elements

    fallback = []
    seen = set()
    for body in soup.select("div.resbody, [itemprop='commentText'], dd.body"):
        container = None
        for parent in body.parents:
            if getattr(parent, "name", None) in ("body", "html"):
                break
            if _looks_like_response_container(parent):
                container = parent
                break
        if container is None:
            container = body.parent or body

        marker = id(container)
        if marker not in seen:
            seen.add(marker)
            fallback.append(container)
    return fallback


def _find_body_tag(element):
    classes = element.get("class") or []
    if element.name == "div" and "resbody" in classes:
        return element
    if element.get("itemprop") == "commentText":
        return element
    if element.name == "dd" and "body" in classes:
        nested = element.select_one("div.resbody, [itemprop='commentText']")
        return nested or element

    for selector in (
        "dd.body > div.resbody",
        "div.resbody",
        "[itemprop='commentText']",
        "dd.body",
    ):
        body_tag = element.select_one(selector)
        if body_tag is not None:
            return body_tag
    return None


def _parse_posts_from_soup(soup: BeautifulSoup) -> List[ScrapedPost]:
    posts: List[ScrapedPost] = []
    for element in _select_response_elements(soup):
        post_no = _extract_post_no(element)

        time_tag = element.select_one("span[itemprop='commentTime']")
        if time_tag is None:
            time_tag = element.select_one("time")
        if time_tag is None:
            time_tag = element.select_one(".resdate, .res_date")
        posted_at = time_tag.get_text(" ", strip=True) if time_tag else None

        body_tag = _find_body_tag(element)
        if body_tag is None:
            continue

        raw_text = body_tag.get_text(separator="\n", strip=True)
        lines = [line.strip() for line in raw_text.splitlines()]
        body_text = "\n".join(line for line in lines if line)
        if not body_text:
            continue

        posts.append(
            ScrapedPost(
                post_no=post_no,
                posted_at=posted_at,
                body=body_text,
                anchors=extract_anchors(body_text),
            )
        )
    return posts


def _fetch_single_page(session: requests.Session, url: str, headers: dict) -> List[ScrapedPost]:
    """指定URLから1ページ分のレスを取得する。"""
    try:
        response = session.get(url, headers=headers, timeout=10)
    except Exception as exc:
        raise ScrapingError(f"ページ取得に失敗しました: {exc}")

    if response.status_code != 200:
        raise ScrapingError(f"HTTPステータスコードが異常です: {response.status_code}")

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    posts = _parse_posts_from_soup(soup)

    if not posts:
        for attempt in range(3):
            time.sleep(0.6 * (attempt + 1) + random.uniform(0.0, 0.4))
            try:
                retry = session.get(url, headers=headers, timeout=10)
            except Exception:
                continue
            if retry.status_code != 200:
                continue

            retry_soup = BeautifulSoup(retry.text, "html.parser")
            retry_posts = _parse_posts_from_soup(retry_soup)
            if retry_posts:
                response = retry
                html = retry.text
                soup = retry_soup
                posts = retry_posts
                break

    if not posts:
        try:
            title = soup.title.string.strip() if soup.title and soup.title.string else ""
        except Exception:
            title = ""
        logging.info(
            "[SCRAPER][empty_res] url=%s final_url=%s status=%s title=%s len=%d "
            "resbody=%d comment_text=%d dd_body=%d",
            url,
            getattr(response, "url", ""),
            getattr(response, "status_code", None),
            title,
            len(html) if html else 0,
            len(soup.select("div.resbody")),
            len(soup.select("[itemprop='commentText']")),
            len(soup.select("dd.body")),
        )
    return posts


def _page_signature(posts: List[ScrapedPost]) -> tuple:
    """範囲外ページが同じ内容を返す場合に重複ページを検出する。"""
    numbered = tuple(sorted(post.post_no for post in posts if post.post_no is not None))
    if numbered:
        return ("numbered", numbered)
    return (
        "unknown",
        tuple(sorted((post.posted_at or "", post.body) for post in posts)),
    )


def fetch_posts_from_thread(
    url: str,
    max_pages: int = 20,
    stop_at_post_no: Optional[int] = None,
) -> List[ScrapedPost]:
    """
    ページ指定なし（p=1相当）の最新側から開始し、p=2、p=3...と古い側へ進む。

    - 完走スレは最大20ページ。
    - #1を含むページ、空ページ、同一内容の再返却で終了する。
    - 既存キャッシュがある場合、通常は保存済み最大レス番号に到達したら終了する。
    - プロセス起動後の初回だけ全ページを確認し、旧キャッシュの欠落を補修する。
    """
    base_url = _strip_page_segment(url)
    if not base_url:
        raise ScrapingError("スレURLが空です。")

    safe_max_pages = min(max(1, int(max_pages)), 20)
    session = requests.Session()
    headers = _build_headers()

    repair_required = stop_at_post_no is None or base_url not in _REPAIRED_THREAD_URLS
    all_posts: List[ScrapedPost] = []
    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    seen_page_signatures: set[tuple] = set()

    for page in range(1, safe_max_pages + 1):
        page_url = make_page_url(base_url, page)
        posts = _fetch_single_page(session, page_url, headers)

        if not posts:
            if page == 1 and not all_posts:
                raise ScrapingError("最新ページから投稿らしきテキストが見つかりませんでした。")
            break

        signature = _page_signature(posts)
        if signature in seen_page_signatures:
            break
        seen_page_signatures.add(signature)

        reached_saved_post = False
        reached_oldest_post = False

        for post in posts:
            if post.post_no is not None:
                if post.post_no == 1:
                    reached_oldest_post = True
                if stop_at_post_no is not None and post.post_no <= stop_at_post_no:
                    reached_saved_post = True
                if post.post_no in seen_post_nos:
                    continue
                seen_post_nos.add(post.post_no)
            else:
                unknown_key = (post.posted_at, post.body)
                if unknown_key in seen_unknown:
                    continue
                seen_unknown.add(unknown_key)

            all_posts.append(post)

        logging.info(
            "[SCRAPER][page] url=%s page=%d posts=%d min_no=%s max_no=%s repair=%s",
            base_url,
            page,
            len(posts),
            min((post.post_no for post in posts if post.post_no is not None), default=None),
            max((post.post_no for post in posts if post.post_no is not None), default=None),
            repair_required,
        )

        if reached_oldest_post:
            _REPAIRED_THREAD_URLS.add(base_url)
            break

        if not repair_required and reached_saved_post:
            break

        time.sleep(random.uniform(0.1, 0.2))
    else:
        _REPAIRED_THREAD_URLS.add(base_url)

    if repair_required and all_posts:
        _REPAIRED_THREAD_URLS.add(base_url)

    if not all_posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。")
    return all_posts
