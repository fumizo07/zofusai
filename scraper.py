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


def extract_anchors(text: str) -> List[int]:
    """
    本文中の >>123 のようなアンカーをすべて整数リストで返す。
    重複は一応削除しておきます。
    """
    nums = [int(m.group(1)) for m in anchor_pattern.finditer(text)]
    return sorted(set(nums))


def parse_int_from_text(text: str) -> Optional[int]:
    """
    '#55' や 'res55_block' のような文字列から 55 を取り出す補助関数。
    """
    m = re.search(r"(\d+)", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def make_page_url(base_url: str, page: int) -> str:
    """
    爆サイのスレURLからページ指定付きURLを作る。

    - page == 1 のときは base_url をそのまま使う
    - page >= 2 のときは
        base_url が p=◯ を含んでいれば書き換え、
        含んでいなければ末尾に /p=◯/ を付ける
    """
    url = base_url
    if page == 1:
        return url

    if "p=" in url:
        # 既に p=◯ があれば差し替え
        return re.sub(r"p=\d+", f"p={page}", url)
    else:
        if not url.endswith("/"):
            url += "/"
        return url + f"p={page}/"


def _fetch_single_page(url: str) -> List[ScrapedPost]:
    """
    指定URL（1ページ分）からレス一覧を取得。
    レスがない場合は空リストを返す（エラーにはしない）。
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception as e:
        raise ScrapingError(f"ページ取得に失敗しました: {e}")

    if resp.status_code != 200:
        raise ScrapingError(f"HTTPステータスコードが異常です: {resp.status_code}")

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # --- レスの外側要素（PC版・スマホ版）を拾う ---

    # PC版: <dl id="res_list"> 内の div.article.res_list_article
    res_elems = soup.select("dl#res_list div.article.res_list_article")

    # スマホ版: <ul id="res_list"> 内の li.res_block
    if not res_elems:
        res_elems = soup.select("ul#res_list li.res_block")

    if not res_elems:
        # このページにはレスがない
        return []

    posts: List[ScrapedPost] = []

    for el in res_elems:
        # --- レス番号 ---
        post_no: Optional[int] = None
        resno_a = el.select_one("span.resnumb a")
        if resno_a:
            post_no = parse_int_from_text(resno_a.get_text(strip=True))

        if post_no is None:
            el_id = el.get("id") or ""
            post_no = parse_int_from_text(el_id)

        # --- 投稿日時 ---
        time_tag = el.select_one("span[itemprop='commentTime']")
        posted_at = time_tag.get_text(strip=True) if time_tag else None

        # --- 本文 ---
        body_tag = el.select_one("dd.body > div.resbody")
        if body_tag is None:
            body_tag = el.select_one("div.resbody")

        if not body_tag:
            continue

        raw_text = body_tag.get_text(separator="\n", strip=True)

        # 各行ごとに strip して先頭の変なスペースを消す
        lines = [line.strip() for line in raw_text.splitlines()]
        body_text = "\n".join(line for line in lines if line)

        if not body_text:
            continue

        anchors = extract_anchors(body_text)

        posts.append(
            ScrapedPost(
                post_no=post_no,
                posted_at=posted_at,
                body=body_text,
                anchors=anchors,
            )
        )

    return posts


def fetch_posts_from_thread(url: str, max_pages: int = 20) -> List[ScrapedPost]:
    """
    スレURLから最大 max_pages ページまで巡回してレスを取得する。

    - 1ページ目: 渡された URL をそのまま使う
    - 2ページ目以降: /p=2/, /p=3/, ... のように URL を変えて取得
    - あるページでレスが 0 件になったらそこで打ち切り
    """
    all_posts: List[ScrapedPost] = []

    for page in range(1, max_pages + 1):
        page_url = make_page_url(url, page)
        posts = _fetch_single_page(page_url)

        if not posts:
            # 1ページ目からして空なら「そもそもスレを読めていない」と判断
            if page == 1:
                raise ScrapingError("投稿らしきテキストが見つかりませんでした。")
            break

        all_posts.extend(posts)

        # マナーとしてちょっと待つ（爆速連打防止）
        time.sleep(1)

    if not all_posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。")

    return all_posts
