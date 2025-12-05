from typing import List

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates

app = FastAPI()

# テンプレートの場所を指定
templates = Jinja2Templates(directory="templates")

# 仮の投稿データ（あとでDBやスクレイピングに置き換える）
FAKE_POSTS = [
    {"id": 1, "text": "これはテスト投稿1です。キーワード：テスト"},
    {"id": 2, "text": "これはテスト投稿2です。キーワード：検索ツール"},
    {"id": 3, "text": "これはテスト投稿3です。アンカー >>1 の例。"},
]


@app.get("/", response_class=HTMLResponse)
async def show_search_page(request: Request, q: str = ""):
    """
    ルート画面。
    クエリパラメータ q があれば簡易検索して結果を表示します。
    例: /?q=テスト
    """
    keyword = q.strip()
    results: List[dict] = []

    if keyword:
        results = [p for p in FAKE_POSTS if keyword in p["text"]]

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword,
            "results": results,
        },
    )


@app.get("/api/search")
async def api_search(q: str) -> List[dict]:
    """
    JSONで結果を返すAPI版。
    例: /api/search?q=テスト
    """
    keyword = q.strip()
    if not keyword:
        return []

    results = [p for p in FAKE_POSTS if keyword in p["text"]]
    return results
