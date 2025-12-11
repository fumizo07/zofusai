import re
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup


# 爆サイ「大阪風俗・お店」ランキングを表示しているページ
BAKUSAI_RANKING_URL = (
    "https://bakusai.com/thr_tl/acode=7/ctgid=103/bid=410/"
)

# アクセスしすぎ防止用：同じプロセス内ではこの時間だけキャッシュする
CACHE_TTL = timedelta(minutes=15)


class RankingError(Exception):
    """ランキング取得に失敗したとき用の例外"""
    pass


# シンプルなメモリキャッシュ
_cache_data = None
_cache_time: datetime | None = None


def get_osaka_fuzoku_ranking(force_refresh: bool = False) -> dict | None:
    """
    爆サイの「大阪風俗 ランキング」から
    - おすすめ
    - 総合アクセス
    - 急上昇
    をそれぞれ上位5件ずつ取得して返す。

    戻り値の例:
    {
        "osusume": [{"rank": 1, "title": "コンカフェ×オナクラ あいこねくと"}, ...],
        "access":  [...],
        "up":      [...]
    }

    失敗した場合は None を返す。
    """
    global _cache_data, _cache_time

    # キャッシュが有効ならそれを返す
    if not force_refresh and _cache_data is not None and _cache_time is not None:
        if datetime.utcnow() - _cache_time < CACHE_TTL:
            return _cache_data

    try:
        ranking = _fetch_and_parse()
    except RankingError:
        # 取得に失敗した場合はキャッシュを壊さず None を返す
        return None

    _cache_data = ranking
    _cache_time = datetime.utcnow()
    return ranking


def _fetch_and_parse() -> dict:
    """
    実際に爆サイへ HTTP アクセスして、ランキング部分をパースする内部関数。
    """
    try:
        resp = requests.get(
            BAKUSAI_RANKING_URL,
            timeout=10,
            headers={
                # ごく普通の UA を名乗っておく（過度に目立たないように）
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        resp.raise_for_status()
    except Exception as e:
        raise RankingError(f"HTTP error: {e}") from e

    # HTML をパースしてテキストだけ取り出す
    soup = BeautifulSoup(resp.text, "html.parser")
    full_text = soup.get_text("\n", strip=True)

    # 「大阪風俗 ランキング」以降だけを対象にする
    idx = full_text.find("大阪風俗 ランキング")
    if idx == -1:
        raise RankingError("ランキング見出しが見つかりませんでした。")

    segment = full_text[idx:]

    # 「11位以下を見る」や「この掲示板のURL」などで区切っておく（安全側）
    cut_marks = ["11位以下を見る", "この掲示板のURL"]
    cut_positions = [segment.find(m) for m in cut_marks if m in segment]
    if cut_positions:
        segment = segment[: min(cut_positions)]

    lines = [line.strip() for line in segment.splitlines() if line.strip()]

    try:
        osusume_idx = lines.index("おすすめ")
        access_idx = lines.index("総合アクセス")
        up_idx = lines.index("急上昇")
    except ValueError as e:
        raise RankingError("カテゴリ見出しが見つかりませんでした。") from e

    osusume, _ = _extract_category(lines, osusume_idx + 1, max_items=5)
    access, _ = _extract_category(lines, access_idx + 1, max_items=5)
    up, _ = _extract_category(lines, up_idx + 1, max_items=5)

    if not (osusume or access or up):
        raise RankingError("ランキングのパースに失敗しました。")

    return {
        "osusume": osusume,
        "access": access,
        "up": up,
    }


def _extract_category(lines: list[str], start_idx: int, max_items: int = 5):
    """
    1カテゴリ分（おすすめ / 総合アクセス / 急上昇）をパースする。

    - 行のうち「閲覧数」「レス数」を両方含むものを1店舗とみなす
    - 先頭の数字を順位、それ以降「閲覧数」までを店舗名として抜き出す
    """
    items: list[dict] = []
    i = start_idx

    while i < len(lines):
        line = lines[i]

        # 次のカテゴリ見出しに到達したら終了
        if line in ("おすすめ", "総合アクセス", "急上昇"):
            break

        # 「12月10日 11:23 更新」などの更新行で終了（既に何件か取れていたら）
        if "更新" in line and "月" in line:
            if items:
                break
            i += 1
            continue

        if "閲覧数" in line and "レス数" in line:
            m = re.search(r"^(\d+)\s+(.+?)\s+閲覧数", line)
            if m:
                rank = int(m.group(1))
                raw_title = m.group(2).strip()
                title = _clean_title(raw_title)
                items.append({"rank": rank, "title": title})
                if len(items) >= max_items:
                    break

        i += 1

    return items, i


def _clean_title(raw: str) -> str:
    """
    行の中から「店舗名」っぽい部分だけを抜き出すための軽い整形。

    例:
        '1 コンカフェ×オナクラ あいこねくと 22' → 'コンカフェ×オナクラ あいこねくと'
        '3 コンカフェ×オナクラ あいこねくと 梅田店 ⑥' → 'コンカフェ×オナクラ あいこねくと 梅田店'
    """
    parts = raw.split()
    if not parts:
        return raw

    # 末尾が純粋な数字 or ①②③…のような丸数字なら落とす
    if re.fullmatch(r"\d+", parts[-1]) or re.fullmatch(r"[①-⑳]", parts[-1]):
        parts = parts[:-1]

    return " ".join(parts)

# デバッグ用：あとで消してOK
print("ranking status:", resp.status_code, file=sys.stderr)
print("ranking snippet:", resp.text[:500], file=sys.stderr)

