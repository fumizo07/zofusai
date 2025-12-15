import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

from app.services.text_utils import normalize_for_search

def _normalize_bakusai_href(href: str) -> str:
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://bakusai.com" + href
    return href

def _is_valid_bakusai_thread_url(u: str) -> bool:
    try:
        p = urlparse(u)
    except Exception:
        return False

    if p.scheme not in ("http", "https"):
        return False

    host = (p.netloc or "").lower()
    if host not in ("bakusai.com", "www.bakusai.com"):
        return False

    path = p.path or ""
    if "/thr_res/" not in path:
        return False

    return True

def find_prev_next_thread_urls(thread_url: str, area_code: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        resp = requests.get(
            thread_url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
    except Exception:
        return (None, None)

    soup = BeautifulSoup(resp.text, "html.parser")
    pager = soup.find("div", id="thr_pager")
    if not pager:
        return (None, None)

    prev_div = pager.find("div", class_="sre_mae")
    next_div = pager.find("div", class_="sre_tsugi")

    def pick_url(div) -> Optional[str]:
        if not div:
            return None
        a = div.find("a", href=True)
        if not a:
            return None
        href = a.get("href", "")
        if not href:
            return None
        return _normalize_bakusai_href(href)

    prev_url = pick_url(prev_div)
    next_url = pick_url(next_div)
    return (prev_url, next_url)

def search_threads_external(
    area_code: str,
    keyword: str,
    max_days: Optional[int],
    board_category: str = "",
    board_id: str = "",
) -> List[dict]:
    keyword = (keyword or "").strip()
    area_code = (area_code or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    if not area_code or not keyword:
        return []

    base = f"https://bakusai.com/sch_thr_thread/acode={area_code}/"
    if board_category:
        base += f"ctgid={board_category}/"
    if board_id:
        base += f"bid={board_id}/"

    url = (
        base
        + "p=1/sch=thr_sch/sch_range=board/word="
        + quote_plus(keyword)
        + "/"
    )

    resp = requests.get(
        url,
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    threads: List[dict] = []

    threshold: Optional[datetime] = None
    if max_days is not None:
        threshold = datetime.now() - timedelta(days=max_days)

    keyword_norm = normalize_for_search(keyword)

    for s in soup.find_all(string=re.compile("最新レス投稿日時")):
        text = str(s)
        m = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", text)
        if not m:
            continue
        try:
            dt = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M")
        except ValueError:
            continue

        if threshold is not None and dt < threshold:
            continue

        parent = s.parent
        link = None
        while parent is not None and parent.name not in ("html", "body"):
            candidate = parent.find("a", href=True)
            if candidate and "/thr_res/" in candidate.get("href", ""):
                link = candidate
                break
            parent = parent.parent

        if not link:
            continue

        title = (link.get_text() or "").strip()
        if not title:
            continue

        title_norm = normalize_for_search(title)
        if keyword_norm not in title_norm:
            continue

        href = link.get("href", "")
        if not href:
            continue

        full_url = _normalize_bakusai_href(href)

        threads.append(
            {
                "title": title,
                "url": full_url,
                "last_post_at_str": dt.strftime("%Y-%m-%d %H:%M"),
            }
        )

    unique_by_url: Dict[str, dict] = {}
    for t in threads:
        if t["url"] not in unique_by_url:
            unique_by_url[t["url"]] = t

    result = list(unique_by_url.values())
    result.sort(key=lambda x: x.get("last_post_at_str") or "", reverse=True)
    return result
