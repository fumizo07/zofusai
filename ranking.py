import logging
from dataclasses import dataclass
from typing import List, Dict, Optional

# 将来スクレイピングで使う予定の import はとりあえずコメントアウトしてOK
# import requests
# from bs4 import BeautifulSoup


@dataclass
class RankingItem:
    name: str
    url: Optional[str] = None


def get_osaka_fuzoku_ranking() -> Dict[str, object]:
    """
    爆サイ大阪デリヘルランキングを返す予定の関数。
    まずは配線確認用にダミーデータを返す。
    戻り値のフォーマットはテンプレート側で使う前提に合わせておく。
    """
    try:
        # ★ここではまだスクレイピングしないで固定値を返す
        data = {
            "osusume": [
                RankingItem(name="ダミー店 おすすめ1", url="https://example.com/osusume1"),
                RankingItem(name="ダミー店 おすすめ2", url="https://example.com/osusume2"),
            ],
            "sogo": [
                RankingItem(name="ダミー店 総合1", url="https://example.com/sogo1"),
                RankingItem(name="ダミー店 総合2", url="https://example.com/sogo2"),
            ],
            "kyujo": [
                RankingItem(name="ダミー店 急上昇1", url="https://example.com/kyujo1"),
                RankingItem(name="ダミー店 急上昇2", url="https://example.com/kyujo2"),
            ],
            # エラーメッセージ用フィールド（正常時は None）
            "error": None,
        }
        logging.info("DEBUG get_osaka_fuzoku_ranking dummy data: %s", data)
        return data
    except Exception as e:
        logging.exception("ランキング取得に失敗しました: %s", e)
        # 失敗時も None を返さず、必ず dict を返すようにしておく
        return {
            "osusume": [],
            "sogo": [],
            "kyujo": [],
            "error": "ランキングの取得に失敗しました。",
        }
