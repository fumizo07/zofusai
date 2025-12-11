import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag, NavigableString

# 爆サイの「大阪デリヘル ランキング」が載っているスレ
RANKING_SOURCE_URL = (
    "https://bakusai.com/thr_tl/acode=7/ctgid=103/bid=5922/"
)

CACHE_TTL = timedelta(minutes=30)


@dataclass
class RankingItem:
    name: str
    url: str


@dataclass
class OsakaRanking:
    osusume: List[RankingItem]
    sogo: List[RankingItem]
    kyujo: List[RankingItem]
    error: Optional[str] = None


_cache: Optional[OsakaRanking] = None
_cache_time: Optional[datetime] = None


def _parse_ranking_links(soup: BeautifulSoup) -> OsakaRanking:
    """
    爆サイのページから「大阪デリヘル ランキング」ブロックを見つけて、
    「おすすめ / 総合アクセス / 急上昇」の各ランキングを抽出します。
    """
    # alt に「大阪デリヘル ランキング」を含む img を起点にする
    img = soup.find("img", alt=re.compile("大阪デリヘル ランキング"))
    if not img:
        raise ValueError("ランキング画像(img alt*='大阪デリヘル ランキング')が見つかりませんでした。")

    rank_links: List[Tag] = []

    # 画像の後ろ側を順に見ていき、「11位以下を見る」が出てきたら終了
    for el in img.next_elements:
        # テキストに「11位以下を見る」が出たらランキングブロック終端
        if isinstance(el, NavigableString):
            txt = str(el).strip()
            if not txt:
                continue
            if "11位以下を見る" in txt:
                break
            continue

        if not isinstance(el, Tag):
            continue

        if el.name != "a":
            continue

        text = el.get_text(" ", strip=True)
        if not text:
            continue

        # ランキング行は「閲覧数」「レス数」を含む
        if "閲覧数" in text and "レス数" in text:
            rank_links.append(el)

    if len(rank_links) < 3:
        raise ValueError(f"ランキング用リンクが想定より少ないです: {len(rank_links)}件")

    total = len(rank_links)
    # 通常は 30 件（10×3カテゴリ）だが、念のため 3 で割ってブロックを推定
    chunk = total // 3
    if chunk == 0:
        raise ValueError(f"ランキングリンク数からカテゴリ分割ができません: total={total}")

    def to_items(links: List[Tag]) -> List[RankingItem]:
        items: List[RankingItem] = []

        # 上位 5 件だけ使う
        for a in links[:5]:
            text = a.get_text(" ", strip=True)

            # 例: "1 カーサビアンカ⑧ 閲覧数 36.3万 レス数 7,552"
            m = re.search(r"\d+\s+(.+?)\s+閲覧数", text)
            if m:
                name = m.group(1)
            else:
                # 失敗したら「閲覧数」以降を削るだけの雑なフォールバック
                name = text
                idx = name.find("閲覧数")
                if idx != -1:
                    name = name[:idx].strip()

            href = a.get("href") or ""
            if href and not href.startswith("http"):
                href = "https://bakusai.com" + href

            if not href:
                # どうしても URL が取れない場合は元ページに飛ばす
                href = RANKING_SOURCE_URL

            items.append(RankingItem(name=name, url=href))

        return items

    osusume_links = rank_links[0:chunk]
    sogo_links = rank_links[chunk : 2 * chunk]
    kyujo_links = rank_links[2 * chunk : 3 * chunk]

    osusume_items = to_items(osusume_links)
    sogo_items = to_items(sogo_links)
    kyujo_items = to_items(kyujo_links)

    logging.info(
        "爆サイランキング解析: total_links=%d, osusume=%d, sogo=%d, kyujo=%d",
        total,
        len(osusume_items),
        len(sogo_items),
        len(kyujo_items),
    )

    return OsakaRanking(
        osusume=osusume_items,
        sogo=sogo_items,
        kyujo=kyujo_items,
        error=None,
    )


def _fetch_from_web() -> OsakaRanking:
    """
    爆サイのページにアクセスしてランキングを取得。
    失敗した場合はダミーデータ＋errorメッセージ付きで返す。
    """
    try:
        headers = {
            # 適当なブラウザっぽい UA を付けておく（403 対策）
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        resp = requests.get(RANKING_SOURCE_URL, headers=headers, timeout=10)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        ranking = _parse_ranking_links(soup)
        return ranking

    except Exception as e:
        logging.exception("爆サイランキング取得でエラーが発生しました。")

        # ここで返すダミーは「サイトが落ちてる or 仕様変更」のときの保険
        dummy = OsakaRanking(
            osusume=[
                RankingItem("ダミー店 おすすめ1", "https://example.com/osusume1"),
                RankingItem("ダミー店 おすすめ2", "https://example.com/osusume2"),
            ],
            sogo=[
                RankingItem("ダミー店 総合1", "https://example.com/sogo1"),
                RankingItem("ダミー店 総合2", "https://example.com/sogo2"),
            ],
            kyujo=[
                RankingItem("ダミー店 急上昇1", "https://example.com/kyujo1"),
                RankingItem("ダミー店 急上昇2", "https://example.com/kyujo2"),
            ],
            error=str(e),
        )
        return dummy


def get_osaka_ranking() -> Optional[OsakaRanking]:
    """
    外部から呼ぶ窓口。
    一定時間（CACHE_TTL）の間はキャッシュを返す。
    """
    global _cache, _cache_time
    now = datetime.utcnow()

    if _cache is not None and _cache_time is not None:
        if now - _cache_time < CACHE_TTL:
            return _cache

    ranking = _fetch_from_web()
    _cache = ranking
    _cache_time = now
    return ranking
