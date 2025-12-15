import re
import unicodedata
from typing import Dict, List, Optional
from collections import defaultdict

from markupsafe import Markup, escape

# =========================
# テキスト整形・検索用ユーティリティ
# =========================
def _normalize_lines(text_value: str) -> str:
    lines = text_value.splitlines()
    cleaned: List[str] = []
    leading = True
    for line in lines:
        if leading and line.strip() == "":
            continue
        line = re.sub(r'^[\s\u3000\xa0]+', '', line)
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
    {"ぁ": "あ", "ぃ": "い", "ぅ": "う", "ぇ": "え", "ぉ": "お"}
)

def normalize_for_search(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = to_hiragana(s)
    s = s.translate(SMALL_KANA_MAP)
    s = s.lower()
    return s

def _build_highlight_variants(keyword: str) -> List[str]:
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

_EMOJI_PATTERN = re.compile(
    "["  # ざっくり emoji / 記号レンジ
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

def remove_emoji(text: str) -> str:
    return _EMOJI_PATTERN.sub("", text or "")

def build_store_search_title(title: str) -> str:
    if not title:
        return ""
    t = simplify_thread_title(title)
    t = remove_emoji(t)
    t = re.sub(r"[\s　]*[★☆◇◆◎○●⚫⚪※✕✖️✖︎-]*\s*\d{1,3}\s*$", "", t)
    return t.strip()

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

def build_reply_tree(all_posts, root) -> List[dict]:
    replies: Dict[int, List] = defaultdict(list)
    for p in all_posts:
        for a in parse_anchors_csv(getattr(p, "anchors", None)):
            replies[a].append(p)

    result: List[dict] = []
    visited_ids: set[int] = set()

    def dfs(post, depth: int) -> None:
        pid = getattr(post, "id", None)
        if pid is not None and pid in visited_ids:
            return
        if pid is not None:
            visited_ids.add(pid)

        if getattr(post, "id", None) != getattr(root, "id", None):
            result.append({"post": post, "depth": depth})

        post_no = getattr(post, "post_no", None)
        if post_no is None:
            return

        for child in replies.get(post_no, []):
            dfs(child, depth + 1)

    root_no = getattr(root, "post_no", None)
    if root_no is not None:
        for child in replies.get(root_no, []):
            dfs(child, 0)

    return result

def linkify_anchors_in_html(thread_url: str, html: str) -> Markup:
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
            f'<a href="{href}" target="_blank" '
            f'rel="nofollow noopener noreferrer">&gt;&gt;{no}</a>'
        )

    linked = re.sub(r"&gt;&gt;(\d+)", repl, html)
    return Markup(linked)

def highlight_with_links(text_value: Optional[str], keyword: str, thread_url: str) -> Markup:
    highlighted = highlight_text(text_value, keyword)
    return linkify_anchors_in_html(thread_url, str(highlighted))
