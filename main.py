from fastapi import FastAPI
from typing import List

app = FastAPI()


# 仮の投稿データ（あとでDBやスクレイピングに置き換える）
FAKE_POSTS = [
    {"id": 1, "text": "これはテスト投稿1です。キーワード：爆サイ"},
    {"id": 2, "text": "これはテスト投稿2です。キーワード：検索ツール"},
    {"id": 3, "text": "これはテスト投稿3です。アンカー >>1 の例。"},
]


@app.get("/")
def read_root():
    return {"message": "My Personal Baku Search API"}


@app.get("/search")
def search(q: str) -> List[dict]:
    """
    /search?q=爆サイ のようにアクセスすると、
    テキストに q が含まれる投稿だけ返します。
    """
    q = q.strip()
    if not q:
        return []

    result = [p for p in FAKE_POSTS if q in p["text"]]
    return result
