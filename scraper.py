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


def make_page_url(base_url: str, page: int) -> str:
    """
    掲示板のスレURLからページ指定付きURLを作る。

    - page == 1 のときは base_url をそのまま使う
    - page >= 2 のときは
        base_url が p=◯ を含んでいれば書き換え、
        含んでいなければ末尾に /p=◯/ を付ける
    """
    url = base_url
    if page == 1:
        return url

    if "p=" in url:
        return re.sub(r"p=\d+", f"p={page}", url)

    if not url.endswith("/"):
        url += "/"
    return url + f"p={page}/"


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


def _select_response_elements(soup: BeautifulSoup):
    """PC版・スマホ版・軽微なクラス変更を吸収してレス外側要素を探す。"""
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
    return []


def _fetch_single_page(session: requests.Session, url: str, headers: dict) -> List[ScrapedPost]:
    """
    指定URL（1ページ分）からレス一覧を取得。
    レスがない場合は空リストを返す。
    """
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
                "[SCRAPER][empty_res] url=%s status=%s title=%s len=%d",
                url,
                getattr(resp, "status_code", None),
                title,
                len(html) if html else 0,
            )
            return []

    posts: List[ScrapedPost] = []

    for el in res_elems:
        post_no = _extract_post_no(el)

        time_tag = el.select_one("span[itemprop='commentTime']")
        if time_tag is None:
            time_tag = el.select_one("time")
        if time_tag is None:
            time_tag = el.select_one(".resdate, .res_date")
        posted_at = time_tag.get_text(" ", strip=True) if time_tag else None

        body_tag = el.select_one("dd.body > div.resbody")
        if body_tag is None:
            body_tag = el.select_one("div.resbody")
        if body_tag is None:
            body_tag = el.select_one("[itemprop='commentText']")
        if body_tag is None:
            body_tag = el.select_one("dd.body")

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


def fetch_posts_from_thread(
    url: str,
    max_pages: int = 20,
    stop_at_post_no: Optional[int] = None,
) -> List[ScrapedPost]:
    """
    スレURLから最大 max_pages ページまで巡回してレスを取得する。

    stop_at_post_no が指定された場合は、保存済み最大レス番号以下のレスを含む
    ページに到達した時点で終了する。これにより通常更新は新着ページだけで済む。

    途中1ページだけ空になった場合は次ページも確認し、2ページ連続で空に
    なった時点で終端と判断する。
    """
    all_posts: List[ScrapedPost] = []
    session = requests.Session()
    headers = _build_headers()

    seen_post_nos: set[int] = set()
    seen_unknown: set[tuple[Optional[str], str]] = set()
    consecutive_empty_pages = 0

    for page in range(1, max_pages + 1):
        page_url = make_page_url(url, page)
        posts = _fetch_single_page(session, page_url, headers)

        if not posts:
            if page == 1:
                raise ScrapingError("投稿らしきテキストが見つかりませんでした。")

            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 2:
                break
            continue

        consecutive_empty_pages = 0
        reached_saved_post = False

        for post in posts:
            if post.post_no is not None:
                if post.post_no in seen_post_nos:
                    continue
                seen_post_nos.add(post.post_no)
                if stop_at_post_no is not None and post.post_no <= stop_at_post_no:
                    reached_saved_post = True
            else:
                unknown_key = (post.posted_at, post.body)
                if unknown_key in seen_unknown:
                    continue
                seen_unknown.add(unknown_key)

            all_posts.append(post)

        if stop_at_post_no is not None and reached_saved_post:
            break

        time.sleep(random.uniform(0.1, 0.2))

    if not all_posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。")

    return all_posts
