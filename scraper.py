# 001
# scraper.py
import re
import time
import random
import logging

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


@dataclass
class PageFetchResult:
    posts: List[ScrapedPost]
    current_page: Optional[int]
    last_page: Optional[int]


anchor_pattern = re.compile(r">>(\d+)")


def extract_anchors(text: str) -> List[int]:
    """
    本文中の >>123 のようなアンカーをすべて整数リストで返す。
    重複は削除する。
    """
    nums = [int(m.group(1)) for m in anchor_pattern.finditer(text)]
    return sorted(set(nums))


def parse_int_from_text(text: str) -> Optional[int]:
    """
    '#55' や 'res55_block' のような文字列から 55 を取り出す補助関数。
    """
    m = re.search(r"(\d+)", text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _strip_page_segment(url: str) -> str:
    """スレURLから /p=数字/ を除き、ページ指定なしのURLへ戻す。"""
    base = re.sub(r"/p=\d+/?", "/", (url or "").strip())
    base = re.sub(r"/{2,}", "/", base.replace("https://", "https:__SLASH__"))
    base = base.replace("https:__SLASH__", "https://")
    if base and not base.endswith("/"):
        base += "/"
    return base


def make_page_url(base_url: str, page: int) -> str:
    """
    爆サイのページ番号は古い側から p=1, p=2... と進む。
    ページ指定なしURLは最新側を表示するため、p=1も必ず明示する。
    """
    base = _strip_page_segment(base_url)
    return base + f"p={max(1, int(page))}/"


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
    """
    スレッドページのタイトル文字列を取得する。
    失敗した場合は None を返す。
    """
    headers = _build_headers()
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception:
        return None

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    if soup.title and soup.title.string:
        return soup.title.string.strip()

    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
        if title:
            return title

    return None


def _parse_post_no_candidate(value: Optional[str]) -> Optional[int]:
    """レス番号候補から、過度に広い数字抽出を避けつつ番号を取り出す。"""
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


def _extract_post_no(el) -> Optional[int]:
    """
    爆サイ側のHTML表現揺れを考慮してレス番号を取得する。
    日時などの無関係な数字を拾わないよう、番号用要素・属性・rridを優先する。
    """
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
        node = el.select_one(selector)
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
        post_no = _parse_post_no_candidate(el.get(attr))
        if post_no is not None:
            return post_no

    for link in el.select("a[href]"):
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


def _find_body_tag(el):
    classes = el.get("class") or []
    if el.name == "div" and "resbody" in classes:
        return el
    if el.get("itemprop") == "commentText":
        return el
    if el.name == "dd" and "body" in classes:
        nested = el.select_one("div.resbody, [itemprop='commentText']")
        return nested or el

    for selector in (
        "dd.body > div.resbody",
        "div.resbody",
        "[itemprop='commentText']",
        "dd.body",
    ):
        body_tag = el.select_one(selector)
        if body_tag is not None:
            return body_tag
    return None


def _extract_pager_state(soup: BeautifulSoup, response_url: str) -> tuple[Optional[int], Optional[int]]:
    """レスページ内のリンクと最終URLから現在ページ・最大ページを取得する。"""
    current_page = None
    page_numbers: set[int] = set()

    match = re.search(r"(?:^|/)p=(\d+)(?:/|$)", response_url or "")
    if match:
        current_page = int(match.group(1))
        page_numbers.add(current_page)

    tid_match = re.search(r"(?:^|/)tid=(\d+)(?:/|$)", response_url or "")
    current_tid = tid_match.group(1) if tid_match else None

    for node in soup.select("[href], [value], [data-page], [data-p]"):
        values = [node.get("href"), node.get("value"), node.get("data-page"), node.get("data-p")]
        for value in values:
            text = str(value or "")
            if not text:
                continue
            if current_tid and "tid=" in text and f"tid={current_tid}" not in text:
                continue

            page_match = re.search(r"(?:^|/)p=(\d+)(?:/|$)", text)
            if page_match:
                page_numbers.add(int(page_match.group(1)))
                continue

            if node.get("data-page") == value or node.get("data-p") == value:
                if text.isdigit():
                    page_numbers.add(int(text))

    last_page = max(page_numbers) if page_numbers else None
    return current_page, last_page


def _fetch_page(session: requests.Session, url: str, headers: dict) -> PageFetchResult:
    """指定URLからレスとページャー情報を取得する。"""
    try:
        resp = session.get(url, headers=headers, timeout=10)
    except Exception as exc:
        raise ScrapingError(f"ページ取得に失敗しました: {exc}")

    if resp.status_code != 200:
        raise ScrapingError(f"HTTPステータスコードが異常です: {resp.status_code}")

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    res_elems = _select_response_elements(soup)

    if not res_elems:
        for attempt in range(3):
            wait_s = 0.6 * (attempt + 1) + random.uniform(0.0, 0.4)
            time.sleep(wait_s)

            try:
                resp2 = session.get(url, headers=headers, timeout=10)
            except Exception:
                continue

            if resp2.status_code != 200:
                continue

            soup2 = BeautifulSoup(resp2.text, "html.parser")
            res_elems = _select_response_elements(soup2)
            if res_elems:
                resp = resp2
                html = resp2.text
                soup = soup2
                break

        if not res_elems:
            try:
                title = soup.title.string.strip() if soup.title and soup.title.string else ""
            except Exception:
                title = ""
            logging.info(
                "[SCRAPER][empty_res] url=%s final_url=%s status=%s title=%s len=%d "
                "resbody=%d comment_text=%d dd_body=%d",
                url,
                getattr(resp, "url", ""),
                getattr(resp, "status_code", None),
                title,
                len(html) if html else 0,
                len(soup.select("div.resbody")),
                len(soup.select("[itemprop='commentText']")),
                len(soup.select("dd.body")),
            )
            current_page, last_page = _extract_pager_state(soup, getattr(resp, "url", url))
            return PageFetchResult([], current_page, last_page)

    posts: List[ScrapedPost] = []

    for el in res_elems:
        post_no = _extract_post_no(el)

        time_tag = el.select_one("span[itemprop='commentTime']")
        if time_tag is None:
            time_tag = el.select_one("time")
        if time_tag is None:
            time_tag = el.select_one(".resdate, .res_date")
        posted_at = time_tag.get_text(" ", strip=True) if time_tag else None

        body_tag = _find_body_tag(el)
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

    current_page, last_page = _extract_pager_state(soup, getattr(resp, "url", url))
    return PageFetchResult(posts, current_page, last_page)


def _fetch_single_page(session: requests.Session, url: str, headers: dict) -> List[ScrapedPost]:
    """既存呼び出しとの互換用。"""
    return _fetch_page(session, url, headers).posts


def fetch_posts_from_thread(
    url: str,
    max_pages: int = 100,
    stop_at_post_no: Optional[int] = None,
) -> List[ScrapedPost]:
    """
    全件取得時は古い側のp=1から最終ページまで巡回する。
    増分取得時は最新ページから古い側へ戻り、保存済みレスに到達したら終了する。

    1ページのレス数は固定ではないため、ページャーから最大ページを更新しながら巡回する。
    max_pagesは異常なページャーによる無限取得を防ぐ安全上限。
    """
    all_posts: List[ScrapedPost] = []
    session = requests.Session()
    headers = _build_headers()
    base_url = _strip_page_segment(url)
    safe_max_pages = max(1, int(max_pages))

    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()

    def add_posts(posts: List[ScrapedPost], *, detect_saved: bool) -> bool:
        reached_saved = False
        for post in posts:
            if post.post_no is not None:
                if post.post_no in seen_post_nos:
                    continue
                seen_post_nos.add(post.post_no)
                if detect_saved and stop_at_post_no is not None and post.post_no <= stop_at_post_no:
                    reached_saved = True
            else:
                unknown_key = (post.posted_at, post.body)
                if unknown_key in seen_unknown:
                    continue
                seen_unknown.add(unknown_key)
            all_posts.append(post)
        return reached_saved

    if stop_at_post_no is None:
        first = _fetch_page(session, make_page_url(base_url, 1), headers)
        if not first.posts:
            raise ScrapingError("1ページ目から投稿らしきテキストが見つかりませんでした。")
        add_posts(first.posts, detect_saved=False)

        target_last_page = min(first.last_page or safe_max_pages, safe_max_pages)
        page = 2
        consecutive_empty_pages = 0

        while page <= target_last_page:
            result = _fetch_page(session, make_page_url(base_url, page), headers)
            if result.last_page:
                target_last_page = min(max(target_last_page, result.last_page), safe_max_pages)

            if not result.posts:
                consecutive_empty_pages += 1
                if first.last_page is None and consecutive_empty_pages >= 2:
                    break
            else:
                consecutive_empty_pages = 0
                add_posts(result.posts, detect_saved=False)

            page += 1
            time.sleep(random.uniform(0.1, 0.2))

    else:
        # 旧実装ではページ指定なしURLだけを1ページ目として扱い、p=1の古いレスが欠けた。
        # 増分取得時もp=1を境界確認し、既存キャッシュの先頭欠落を自然に補修する。
        oldest = _fetch_page(session, make_page_url(base_url, 1), headers)
        if oldest.posts:
            add_posts(oldest.posts, detect_saved=False)

        latest = _fetch_page(session, base_url, headers)
        if not latest.posts:
            raise ScrapingError("最新ページから投稿らしきテキストが見つかりませんでした。")

        latest_page = latest.current_page or latest.last_page
        if latest.current_page is None and latest.last_page and latest.last_page > 1:
            explicit_latest = _fetch_page(session, make_page_url(base_url, latest.last_page), headers)
            if explicit_latest.posts:
                latest = explicit_latest
                latest_page = explicit_latest.current_page or latest.last_page

        if add_posts(latest.posts, detect_saved=True):
            return all_posts

        if latest_page is not None and latest_page > 1:
            for page in range(min(latest_page - 1, safe_max_pages), 1, -1):
                result = _fetch_page(session, make_page_url(base_url, page), headers)
                if result.posts and add_posts(result.posts, detect_saved=True):
                    break
                time.sleep(random.uniform(0.1, 0.2))
        else:
            # ページャーを解析できない場合は、欠落を避けるため全ページ方向へフォールバックする。
            consecutive_empty_pages = 0
            for page in range(2, safe_max_pages + 1):
                result = _fetch_page(session, make_page_url(base_url, page), headers)
                if not result.posts:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        break
                    continue
                consecutive_empty_pages = 0
                add_posts(result.posts, detect_saved=False)
                time.sleep(random.uniform(0.1, 0.2))

    if not all_posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。")

    return all_posts
