# utils.py
import re
import unicodedata
from datetime import datetime
from typing import List, Optional, Dict

from markupsafe import Markup, escape


# =========================
# テキスト整形・検索用ユーティリティ
# =========================
def _normalize_lines(text_value: str) -> str:
    """
    余計な行頭全角スペース・空行を削除して、見やすい形に整える
    """
    lines = text_value.splitlines()
    cleaned: List[str] = []
    leading = True
    for line in lines:
        if leading and line.strip() == "":
            continue
        line = re.sub(r"^[\s\u3000\xa0]+", "", line)
        cleaned.append(line)
        leading = False
    return "\n".join(cleaned)


def to_hiragana(s: str) -> str:
    result = []
    for ch in s:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            result.append(chr(code - 0x60))
        else:
            result.append(ch)
    return "".join(result)


def to_katakana(s: str) -> str:
    result = []
    for ch in s:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            result.append(chr(code + 0x60))
        else:
            result.append(ch)
    return "".join(result)


SMALL_KANA_MAP = str.maketrans(
    {
        "ぁ": "あ",
        "ぃ": "い",
        "ぅ": "う",
        "ぇ": "え",
        "ぉ": "お",
    }
)


def normalize_for_search(s: Optional[str]) -> str:
    """
    検索用の正規化：
    - NFKC
    - カタカナ → ひらがな
    - 小書き母音（ぁぃぅぇぉ）を通常のあいうえおに揃える
    - 小文字化
    """
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = to_hiragana(s)
    s = s.translate(SMALL_KANA_MAP)
    s = s.lower()
    return s


# =========================
# タグ用ユーティリティ（★追加）
# =========================
def normalize_tag_token(s: str) -> str:
    """
    タグ1個の正規化：
    - NFKC
    - 両端空白除去（半角/全角）
    - 連続空白を1つに圧縮
    - 小文字化（英数字だけ効く想定）
    """
    s = unicodedata.normalize("NFKC", s or "")
    s = s.strip().strip("　")
    s = re.sub(r"[\s\u3000]+", " ", s)
    s = s.lower()
    return s.strip()


def parse_tags_input(tags_input: str) -> List[str]:
    """
    入力欄（カンマ区切り）を正規化してトークン配列へ。
    """
    raw = (tags_input or "").strip()
    if not raw:
        return []
    parts = [p for p in raw.split(",")]
    out: List[str] = []
    seen = set()
    for p in parts:
        t = normalize_tag_token(p)
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def tags_list_to_csv(tags: List[str]) -> str:
    """
    DB保存用：カンマ区切り（余計なスペース無し）
    """
    cleaned = [normalize_tag_token(t) for t in (tags or [])]
    cleaned = [t for t in cleaned if t]
    # 重複排除しつつ順序維持
    out: List[str] = []
    seen = set()
    for t in cleaned:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return ",".join(out)


# =========================
# 強調表示
# =========================
def _build_highlight_variants(keyword: str) -> List[str]:
    """
    強調表示用のバリアント生成：
    - NFKC
    - ひらがな / カタカナ両対応
    - 小書き母音（ぁぃぅぇぉ）を通常のあいうえおに揃えた形も含める
    """
    if not keyword:
        return []
    base = unicodedata.normalize("NFKC", keyword)
    hira = to_hiragana(base)
    kata = to_katakana(hira)

    raw_variants = {base, hira, kata}
    expanded: set[str] = set()
    for v in raw_variants:
        if not v:
            continue
        expanded.add(v)
        expanded.add(v.translate(SMALL_KANA_MAP))

    variants = {v for v in expanded if v}
    return sorted(variants, key=len, reverse=True)


def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文の中でキーワード部分を <mark> で囲って強調表示
    （ひらがな/カタカナ/小書き母音の揺れも拾う）
    """
    if text_value is None:
        text_value = ""
    text_value = _normalize_lines(text_value)
    if not keyword:
        return Markup(escape(text_value))

    escaped = escape(text_value)
    variants = _build_highlight_variants(keyword)
    if not variants:
        return Markup(escaped)

    try:
        pattern = re.compile("(" + "|".join(re.escape(v) for v in variants) + ")", re.IGNORECASE)
    except re.error:
        return Markup(escaped)

    def repl(match):
        return Markup(f"<mark>{match.group(0)}</mark>")

    highlighted = pattern.sub(lambda m: repl(m), escaped)
    return Markup(highlighted)


def simplify_thread_title(title: str) -> str:
    if not title:
        return ""
    for sep in ["｜", "|", " - "]:
        if sep in title:
            title = title.split(sep)[0]
    return title.strip()


# =========================
# 店舗ページ検索用：タイトル整形
# =========================
_EMOJI_PATTERN = re.compile(
    "["  # emoji / 記号レンジ（ざっくり）
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "]"
)

_TRAILING_NUMBERLIKE_PATTERN = re.compile(
    r"[\s　]*"
    r"[★☆◇◆◎○●⚫⚪※✕✖️✖︎\-]*"
    r"\s*"
    r"(?:(?:\d{1,4})|(?:[\u2460-\u2473\u24EA\u2776-\u277F]+))"
    r"\s*$"
)


def remove_emoji(text: str) -> str:
    return _EMOJI_PATTERN.sub("", text or "")


def build_store_search_title(title: str) -> str:
    """
    店舗ページ検索用：
    - 絵文字を削除
    - 末尾の「★12」「 12」などのスレ番を削除
    - 末尾の「①②③…」などの丸数字も削除
    """
    if not title:
        return ""
    t = simplify_thread_title(title)
    t = remove_emoji(t)

    while True:
        new_t = _TRAILING_NUMBERLIKE_PATTERN.sub("", t)
        if new_t == t:
            break
        t = new_t

    return t.strip()


def build_google_site_search_url(site: str, query: str) -> str:
    """
    Google で site:xxx を付けて検索する URL を返す
    """
    site = (site or "").strip()
    query = (query or "").strip()
    q = f"site:{site} {query}".strip()
    from urllib.parse import quote_plus
    return "https://www.google.com/search?q=" + quote_plus(q)


# =========================
# アンカー / 日付 / リンク化
# =========================
def parse_anchors_csv(s: Optional[str]) -> List[int]:
    if not s:
        return []
    nums: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if part.isdigit():
            nums.append(int(part))
    return sorted(set(nums))


def parse_posted_at_value(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def linkify_anchors_in_html(thread_url: str, html: str) -> Markup:
    """
    すでに escape / highlight 済みの HTML 文字列内の「&gt;&gt;数字」を
    レス個別ページへのリンクに変換する。
    data-anchor-no を付与
    """
    if not html:
        return Markup("")

    base = thread_url or ""

    m = re.search(
        r"(https://bakusai\.com/thr_res(?:_show)?/acode=\d+/ctgid=\d+/bid=\d+/tid=\d+/)",
        base,
    )
    if m:
        base_rr = m.group(1)
    else:
        base_rr = base

    def repl(match: re.Match) -> str:
        no = match.group(1)
        url = base_rr
        if "thr_res_show" not in url:
            url = url.replace("/thr_res/", "/thr_res_show/")
        if not url.endswith("/"):
            url += "/"
        href = f"{url}rrid={no}/"
        return (
            f'<a class="anchor-link" data-anchor-no="{no}" '
            f'href="{href}" target="_blank" '
            f'rel="nofollow noopener noreferrer">&gt;&gt;{no}</a>'
        )

    linked = re.sub(r"&gt;&gt;(\d+)", repl, html)
    return Markup(linked)


def highlight_with_links(text_value: Optional[str], keyword: str, thread_url: str) -> Markup:
    """
    1) 検索キーワードのハイライト
    2) >>番号 を個別レスへのリンク化
    """
    highlighted = highlight_text(text_value, keyword)
    return linkify_anchors_in_html(thread_url, str(highlighted))
