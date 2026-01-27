# 001
# diary_fetcher_pw.py
import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from playwright.async_api import async_playwright, Browser

JST = timezone(timedelta(hours=9))

# それっぽい日付を拾う（YYYY/MM/DD, YYYY-MM-DD, さらに時刻付きも）
DATE_PATTERNS = [
    re.compile(r"(?P<y>20\d{2})[\/\-\.](?P<m>\d{1,2})[\/\-\.](?P<d>\d{1,2})\s*(?:(?P<h>\d{1,2}):(?P<mi>\d{2}))?"),
    re.compile(r"(?P<y>20\d{2})年(?P<m>\d{1,2})月(?P<d>\d{1,2})日\s*(?:(?P<h>\d{1,2}):(?P<mi>\d{2}))?"),
]

def _to_epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    return int(dt.timestamp() * 1000)

def _parse_candidate_dates(text: str) -> List[int]:
    out: List[int] = []
    if not text:
        return out
    for pat in DATE_PATTERNS:
        for m in pat.finditer(text):
            try:
                y = int(m.group("y"))
                mo = int(m.group("m"))
                d = int(m.group("d"))
                h = int(m.group("h") or 0)
                mi = int(m.group("mi") or 0)
                dt = datetime(y, mo, d, h, mi, tzinfo=JST)
                out.append(_to_epoch_ms(dt))
            except Exception:
                continue
    return out

@dataclass
class _PwState:
    pw: any
    browser: Browser

_pw_state: Optional[_PwState] = None
_pw_lock = asyncio.Lock()

async def _get_browser() -> Browser:
    global _pw_state
    if _pw_state and _pw_state.browser:
        return _pw_state.browser
    async with _pw_lock:
        if _pw_state and _pw_state.browser:
            return _pw_state.browser
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        _pw_state = _PwState(pw=pw, browser=browser)
        return browser

async def close_browser() -> None:
    global _pw_state
    async with _pw_lock:
        if not _pw_state:
            return
        try:
            await _pw_state.browser.close()
        except Exception:
            pass
        try:
            await _pw_state.pw.stop()
        except Exception:
            pass
        _pw_state = None

async def fetch_latest_ts_ms(url: str, timeout_ms: int = 25000) -> Optional[int]:
    """
    外部URL（日記ページ）をPlaywrightで開き、最新の投稿日時(epoch ms)を推定して返す。
    """
    browser = await _get_browser()

    context = await browser.new_context(
        locale="ja-JP",
        timezone_id="Asia/Tokyo",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        },
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # 多少待つ（反ボットや遅延レンダの保険）
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        # 1) <time datetime="..."> を優先
        try:
            dts: List[str] = await page.eval_on_selector_all(
                "time[datetime]",
                "els => els.map(e => e.getAttribute('datetime')).filter(Boolean)"
            )
            cands: List[int] = []
            for s in dts:
                # ISOっぽいのをJSTとして扱う（厳密でないが実務優先）
                try:
                    # 例: 2026-01-26T12:34:00+09:00 / 2026-01-26
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=JST)
                    cands.append(_to_epoch_ms(dt.astimezone(JST)))
                except Exception:
                    cands.extend(_parse_candidate_dates(s))
            if cands:
                return max(cands)
        except Exception:
            pass

        html = await page.content()

        # 2) JSON-LD（構造化データ）から datePublished / dateModified
        try:
            scripts = re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.S | re.I)
            cands: List[int] = []
            for raw in scripts:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue

                def pick(o):
                    if isinstance(o, dict):
                        for k in ("datePublished", "dateModified", "uploadDate"):
                            v = o.get(k)
                            if isinstance(v, str):
                                try:
                                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                                    if dt.tzinfo is None:
                                        dt = dt.replace(tzinfo=JST)
                                    cands.append(_to_epoch_ms(dt.astimezone(JST)))
                                except Exception:
                                    cands.extend(_parse_candidate_dates(v))
                        # 入れ子も探索
                        for vv in o.values():
                            pick(vv)
                    elif isinstance(o, list):
                        for it in o:
                            pick(it)

                pick(obj)

            if cands:
                return max(cands)
        except Exception:
            pass

        # 3) 最後の手段：HTML全体から日付っぽいのを拾って最大値
        cands = _parse_candidate_dates(html)
        if cands:
            return max(cands)

        return None

    finally:
        try:
            await page.close()
        except Exception:
            pass
        try:
            await context.close()
        except Exception:
            pass
