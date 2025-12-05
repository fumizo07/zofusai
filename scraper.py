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
    # 重複を除去してソート
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


def fetch_posts_from_thread(url: str) -> List[ScrapedPost]:
    """
    指定された爆サイのスレURLからレス一覧を取得して返す。

    - PC版:
        <dl id="res_list">
          <div class="article res_list_article" id="res530"> ... </div>
        </dl>

    - スマホ版:
        <ul id="res_list">
          <li id="res530_block" class="res_block"> ... </li>
        </ul>

    本文、レス番号、投稿日時、アンカーを抽出します。
    """

    headers = {
        # ある程度ブラウザに寄せたUser-Agentにしておく
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
        raise ScrapingError("レス要素が見つかりませんでした。HTML構造とセレクタを確認してください。")

    posts: List[ScrapedPost] = []

    for el in res_elems:
        # --- レス番号 ---

        # まずは span.resnumb a のテキスト（例: "#55"）
        post_no: Optional[int] = None
        resno_a = el.select_one("span.resnumb a")
        if resno_a:
            post_no = parse_int_from_text(resno_a.get_text(strip=True))

        # 取れなければ id属性から取得（res55 や res55_block）
        if post_no is None:
            el_id = el.get("id") or ""
            post_no = parse_int_from_text(el_id)

        # --- 投稿日時 ---

        time_tag = el.select_one("span[itemprop='commentTime']")
        posted_at = time_tag.get_text(strip=True) if time_tag else None

        # --- 本文 ---

        # PC版: dd.body > div.resbody
        body_tag = el.select_one("dd.body > div.resbody")
        # スマホ版などで若干違う場合に備えてフォールバックも用意
        if body_tag is None:
            body_tag = el.select_one("div.resbody")

        if not body_tag:
            # 本文がないレスはスキップ
            continue

        body_text = body_tag.get_text(separator="\n", strip=True)
        if not body_text:
            continue

        # --- アンカー抽出（>>123） ---

        anchors = extract_anchors(body_text)

        posts.append(
            ScrapedPost(
                post_no=post_no,
                posted_at=posted_at,
                body=body_text,
                anchors=anchors,
            )
        )

    # あまりに多すぎる場合は安全のため上限
    MAX_POSTS = 1000
    if len(posts) > MAX_POSTS:
        posts = posts[:MAX_POSTS]

    # マナーとしてほんの少し待つ（連続アクセス時の保険）
    time.sleep(1)

    if not posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。")

    return posts
