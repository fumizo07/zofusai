import time
from typing import List

import requests
from bs4 import BeautifulSoup


class ScrapingError(Exception):
    """スクレイピング時のエラー用のカスタム例外"""
    pass


def fetch_posts_from_thread(url: str) -> List[str]:
    """
    指定されたURLからHTMLを取得し、
    「投稿っぽいテキスト」のリストを返します。

    ★重要★
    実際の掲示板ごとに HTML の構造が違うので、
    下の「TODO: セレクタを調整」の部分は自分で調整が必要です。
    最初は雑に <p> や <div> から取ってきて、
    後で「投稿だけ」に絞り込むイメージです。
    """

    headers = {
        # ここは自分の用途に合わせて書き換えてください
        "User-Agent": "PersonalSearchBot/0.1 (+your-email-or-site)"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception as e:
        raise ScrapingError(f"ページ取得に失敗しました: {e}")

    if resp.status_code != 200:
        raise ScrapingError(f"HTTPステータスコードが異常です: {resp.status_code}")

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    posts: List[str] = []

    # ======================================================
    # TODO: ここを実際の掲示板構造に合わせて調整する
    # ======================================================

    # 1. まずは雑に <p> タグを全部拾う簡易版
    #    → 実際には「レス1件分を囲っている div や li」を対象にします。
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text:
            continue

        # ノイズ除去のため、極端に短いものは除外する例
        if len(text) < 5:
            continue

        posts.append(text)

    # 2. あまりに多すぎる時は上限をかけておく（暴走防止）
    MAX_POSTS = 500
    if len(posts) > MAX_POSTS:
        posts = posts[:MAX_POSTS]

    # マナーとして、連続アクセスする場合は少し待つ
    time.sleep(1)

    if not posts:
        raise ScrapingError("投稿らしきテキストが見つかりませんでした。HTML構造とセレクタを確認してください。")

    return posts
