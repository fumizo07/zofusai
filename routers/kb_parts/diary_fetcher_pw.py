# 004
# routers/kb_parts/diary_fetcher_pw.py
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

_JST = timezone(timedelta(hours=9))

# 例: "12/30 23:47"
_RE_MMDD_HHMM = re.compile(r"(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})")

# 例: "2026年1月"
_RE_YEARMON = re.compile(r"(\d{4})年\s*(\d{1,2})月")


def _extract_year_month(text: str) -> Tuple[Optional[int], Optional[int]]:
    m = _RE_YEARMON.search(text or "")
    if not m:
        return None, None
    try:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1900 <= y <= 2100 and 1 <= mo <= 12:
            return y, mo
    except Exception:
        pass
    return None, None


def _guess_year(header_year: Optional[int], header_month: Optional[int], entry_month: int) -> Optional[int]:
    if header_year is None:
        return None
    if header_month is None:
        return header_year
    if header_month == 1 and entry_month == 12:
        return header_year - 1
    return header_year


def _parse_latest_ts_ms_from_text(text: str) -> Tuple[Optional[int], str]:
    if not text:
        return None, "empty_html"

    m = _RE_MMDD_HHMM.search(text)
    if not m:
        return None, "no_datetime_found"

    try:
        mm = int(m.group(1))
        dd = int(m.group(2))
        hh = int(m.group(3))
        mi = int(m.group(4))
        if not (1 <= mm <= 12 and 1 <= dd <= 31 and 0 <= hh <= 23 and 0 <= mi <= 59):
            return None, "datetime_out_of_range"
    except Exception:
        return None, "datetime_parse_error"

    hy, hmo = _extract_year_month(text)
    y = _guess_year(hy, hmo, mm)
    if y is None:
        y = datetime.now(_JST).year

    try:
        dt_jst = datetime(y, mm, dd, hh, mi, 0, tzinfo=_JST)
        dt_utc = dt_jst.astimezone(timezone.utc)
        ts_ms = int(dt_utc.timestamp() * 1000)
        return ts_ms, ""
    except Exception:
        return None, "datetime_to_epoch_failed"


def get_latest_diary_ts_ms(url: str) -> Tuple[Optional[int], str]:
    """
    Returns:
      (latest_ts_ms_utc, err)
      - latest_ts_ms_utc: int milliseconds since epoch (UTC)
      - err: "" on success, otherwise short reason
    """
    u = (url or "").strip()
    if not u:
        return None, "url_empty"

    # 念のため /diary を付ける（呼び出し側がperson_urlでも動くように）
    if not u.rstrip("/").endswith("/diary"):
        u = u.rstrip("/") + "/diary"

    # ★ Playwrightはここで遅延import（サーバー起動時のImportError回避）
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError  # type: ignore
    except Exception as e:
        return None, f"playwright_import_error:{type(e).__name__}"

    nav_timeout_ms = 25_000
    t0 = datetime.now(timezone.utc)

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
                },
                viewport={"width": 1280, "height": 720},
            )

            # webdriver痕跡を軽く潰す（playwright-stealth無しの最低限）
            try:
                context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
            except Exception:
                pass

            page = context.new_page()
            page.set_default_navigation_timeout(nav_timeout_ms)
            page.set_default_timeout(nav_timeout_ms)

            # 重いリソースを切って速度と安定性を上げる
            try:
                def _route_handler(route):
                    r = route.request
                    rt = r.resource_type
                    if rt in ("image", "media", "font"):
                        return route.abort()
                    return route.continue_()

                page.route("**/*", _route_handler)
            except Exception:
                pass

            resp = page.goto(u, wait_until="domcontentloaded")
            status = resp.status if resp is not None else 0

            # --- ★ デバッグ用ログ（403の実体を見る） ---
            final_url = ""
            title = ""
            head200 = ""
            try:
                final_url = page.url or ""
            except Exception:
                final_url = ""
            try:
                title = page.title() or ""
            except Exception:
                title = ""
            try:
                html = page.content() or ""
                head200 = (html[:200].replace("\n", " ").replace("\r", " ").strip())
            except Exception:
                head200 = ""

            # 失敗時だけでなく、状況把握のため常に出す（必要なら後で条件付け）
            try:
                dt = datetime.now(timezone.utc) - t0
                sec = dt.total_seconds()
            except Exception:
                sec = -1
            print(
                f"[diary_pw] goto status={status} sec={sec:.2f} url={u} final_url={final_url} title={title!r} head200={head200!r}"
            )
            # --- ★ ここまでデバッグ用ログ ---

            if status >= 400:
                try:
                    context.close()
                except Exception:
                    pass
                try:
                    browser.close()
                except Exception:
                    pass
                return None, f"http_{status}"

            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass

            html2 = page.content() or ""

            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

            return _parse_latest_ts_ms_from_text(html2)

    except PWTimeoutError:
        return None, "timeout"
    except Exception as e:
        return None, f"playwright_error:{type(e).__name__}"
