# 002
# routers/kb_parts/diary_fetcher_pw.py
from __future__ import annotations

import re
from typing import Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ざっくり「年齢確認っぽい」文言
_RE_AGE_GATE = re.compile(r"(年齢確認|18歳以上|18才以上|あなたは18|成人|このサイトは18)", re.IGNORECASE)

# 「入場/同意」ボタンっぽい
_AGE_OK_TEXTS = [
    "18歳以上",
    "18才以上",
    "はい",
    "同意",
    "入場",
    "ENTER",
    "Yes",
    "OK",
]


def _should_block_resource(url: str) -> bool:
    u = (url or "").lower()
    # 速度重視：画像/フォント/動画/広告系を落とす（HTML解析だけ欲しい）
    return any(
        s in u
        for s in (
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".svg",
            ".woff",
            ".woff2",
            ".ttf",
            ".otf",
            ".mp4",
            ".webm",
            "googletagmanager",
            "doubleclick",
            "google-analytics",
        )
    )


def _try_age_gate(page) -> None:
    """
    年齢確認ページを踏んでいたら、ありそうなボタンを雑に押して本ページへ寄せる。
    失敗しても例外を投げない（ただの最適化）。
    """
    try:
        txt = page.content() or ""
        if not _RE_AGE_GATE.search(txt):
            return
    except Exception:
        return

    # ボタン or リンクで押せそうなものを探す
    try:
        for t in _AGE_OK_TEXTS:
            loc = page.locator(f"button:has-text('{t}')").first
            if loc and loc.is_visible():
                loc.click(timeout=1500)
                return
    except Exception:
        pass

    try:
        for t in _AGE_OK_TEXTS:
            loc = page.locator(f"a:has-text('{t}')").first
            if loc and loc.is_visible():
                loc.click(timeout=1500)
                return
    except Exception:
        pass


def fetch_diary_html(url: str) -> Tuple[str, int, str, str]:
    """
    Returns: (html, status, final_url, err)
      - err == "" on success
      - err examples: "http_403", "timeout", "playwright_error"
    """
    u = (url or "").strip()
    if not u:
        return "", 0, "", "url_empty"

    nav_timeout_ms = 25_000

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                ],
            )

            context = browser.new_context(
                locale="ja-JP",
                timezone_id="Asia/Tokyo",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
            )

            # 軽量化
            try:
                context.route(
                    "**/*",
                    lambda route, request: route.abort()
                    if _should_block_resource(request.url)
                    else route.continue_(),
                )
            except Exception:
                pass

            page = context.new_page()
            page.set_default_navigation_timeout(nav_timeout_ms)
            page.set_default_timeout(nav_timeout_ms)

            resp = page.goto(u, wait_until="domcontentloaded")
            status = resp.status if resp is not None else 0

            # 403/404 などは即返す
            if status >= 400:
                try:
                    final_url = page.url
                except Exception:
                    final_url = ""
                try:
                    browser.close()
                except Exception:
                    pass
                return "", int(status), final_url, f"http_{int(status)}"

            # 年齢確認っぽければ押してみる（成功すれば本ページへ）
            _try_age_gate(page)

            # JSレンダが絡む場合の保険（ただし待ちすぎない）
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass

            html = page.content() or ""
            try:
                final_url = page.url
            except Exception:
                final_url = ""

            try:
                browser.close()
            except Exception:
                pass

            if not html:
                return "", int(status or 0), final_url, "empty_html"

            return html, int(status or 0), final_url, ""

    except PWTimeoutError:
        return "", 0, "", "timeout"
    except Exception:
        return "", 0, "", "playwright_error"
