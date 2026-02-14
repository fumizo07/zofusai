# ranking.py
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString

# ランキングページ URL テンプレート
RANKING_URL_TEMPLATE = "https://bakusai.com/thr_tl/acode={acode}/ctgid={ctgid}/bid={bid}/"

# キャッシュ有効期限
CACHE_TTL = timedelta(minutes=30)


@dataclass
class RankingItem:
    name: str
    url: str


@dataclass
class BoardRanking:
    osusume: List[RankingItem]
    sogo: List[RankingItem]
    kyujo: List[RankingItem]
    error: Optional[str] = None


# 板ごとのキャッシュ
_cache: Dict[Tuple[str, str, str], BoardRanking] = {}
_cache_time: Dict[Tuple[str, str, str], datetime] = {}


def _parse_ranking_links(soup: BeautifulSoup, src_url: str) -> BoardRanking:
    """
    「おすすめ / 総合アクセス / 急上昇」の各ランキングを抽出する。
    """

    # ランキングDL（クラス名は色々付いているので brdRanking を目印にする）
    dl = soup.select_one("dl.brdRanking")
    if not dl:
        raise ValueError("ランキング <dl class='brdRanking'> が見つかりませんでした。")

    tabs = dl.select("div.thr_rankingTab")
    if len(tabs) < 1:
        raise ValueError("ランキングタブ本体 div.thr_rankingTab が見つかりませんでした。")

    tab_osusume = tabs[0] if len(tabs) >= 1 else None
    tab_sogo = tabs[1] if len(tabs) >= 2 else None
    tab_kyujo = tabs[2] if len(tabs) >= 3 else None

    def collect_links(tab: Optional[Tag]) -> List[Tag]:
        if not tab:
            return []
        links: List[Tag] = []
        for a in tab.select("dd > a"):
            text = a.get_text(" ", strip=True)
            if not text:
                continue
            # ランキング行は「閲覧数」「レス数」を含む（仕様変更で外れたらここを緩める）
            if "閲覧数" in text and "レス数" in text:
                links.append(a)
        return links

    def to_items(links: List[Tag]) -> List[RankingItem]:
        items: List[RankingItem] = []
        for a in links[:5]:  # 上位5件だけ表示
            text = a.get_text(" ", strip=True)

            # 新HTMLは .rank_title に店名が入っているのでそれを最優先
            title_el = a.select_one(".rank_title")
            if title_el:
                name = title_el.get_text(" ", strip=True)
            else:
                # フォールバック：従来のテキスト解析
                m = re.search(r"\d+\s+(.+?)\s+閲覧数", text)
                if m:
                    name = m.group(1)
                else:
                    name = text
                    idx = name.find("閲覧数")
                    if idx != -1:
                        name = name[:idx].strip()

            href = a.get("href") or ""
            if href and not href.startswith("http"):
                href = "https://bakusai.com" + href
            if not href:
                href = src_url

            items.append(RankingItem(name=name, url=href))
        return items

    osusume_links = collect_links(tab_osusume)
    sogo_links = collect_links(tab_sogo)
    kyujo_links = collect_links(tab_kyujo)

    osusume_items = to_items(osusume_links)
    sogo_items = to_items(sogo_links)
    kyujo_items = to_items(kyujo_links)

    logging.info(
        "爆サイランキング解析: osusume_links=%d, sogo_links=%d, kyujo_links=%d",
        len(osusume_links),
        len(sogo_links),
        len(kyujo_links),
    )

    return BoardRanking(
        osusume=osusume_items,
        sogo=sogo_items,
        kyujo=kyujo_items,
        error=None,
    )




def _fetch_from_web(acode: str, ctgid: str, bid: str) -> BoardRanking:
    """
    ページにアクセスしてランキングを取得。
    失敗した場合はダミーデータ＋errorメッセージ付きで返す。
    """
    src_url = RANKING_URL_TEMPLATE.format(acode=acode, ctgid=ctgid, bid=bid)

    try:
        headers = {
            # 適当なブラウザっぽい UA を付けておく（403 対策）
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        resp = requests.get(src_url, headers=headers, timeout=10)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        ranking = _parse_ranking_links(soup, src_url)
        return ranking

    except Exception as e:
        logging.exception("爆サイランキング取得でエラーが発生しました。")

        # ここで返すダミーは「サイトが落ちてる or 仕様変更」のときの保険
        dummy = BoardRanking(
            osusume=[
                RankingItem("ダミー店 おすすめ1", src_url),
                RankingItem("ダミー店 おすすめ2", src_url),
            ],
            sogo=[
                RankingItem("ダミー店 総合1", src_url),
                RankingItem("ダミー店 総合2", src_url),
            ],
            kyujo=[
                RankingItem("ダミー店 急上昇1", src_url),
                RankingItem("ダミー店 急上昇2", src_url),
            ],
            error=str(e),
        )
        return dummy


def get_board_ranking(acode: str, ctgid: str, bid: str) -> Optional[BoardRanking]:
    """
    板ごとのランキング取得用窓口。
    - (acode, ctgid, bid) が欠けている場合は None を返す
    - 初回アクセス時は必ずWebから取得
    - 2回目以降は CACHE_TTL の間キャッシュを返す
    """
    acode = (acode or "").strip()
    ctgid = (ctgid or "").strip()
    bid = (bid or "").strip()

    if not acode or not ctgid or not bid:
        return None

    key = (acode, ctgid, bid)
    now = datetime.utcnow()

    if key in _cache and key in _cache_time:
        if now - _cache_time[key] < CACHE_TTL:
            return _cache[key]

    ranking = _fetch_from_web(acode, ctgid, bid)
    _cache[key] = ranking
    _cache_time[key] = now
    return ranking
