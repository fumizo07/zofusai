import os
import re
import unicodedata
import secrets
import json
from typing import List, Optional, Dict
from collections import defaultdict, deque
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlencode

import requests
from bs4 import BeautifulSoup

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles  # 追加

from sqlalchemy import Column, Integer, Text, create_engine, func, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from markupsafe import Markup, escape

from scraper import fetch_posts_from_thread, ScrapingError, get_thread_title

# ランキング（外部検索用）で使うのは後ろの thread_search_page 側だけ
from ranking import get_board_ranking, RANKING_URL_TEMPLATE


# =========================
# DB 初期化
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が設定されていません。環境変数 DATABASE_URL を確認してください。")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ThreadPost(Base):
    __tablename__ = "thread_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)
    thread_title = Column(Text, nullable=True)
    post_no = Column(Integer, nullable=True, index=True)
    posted_at = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    anchors = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)
    memo = Column(Text, nullable=True)


class ThreadMeta(Base):
    """
    スレッド単位のメタ情報（自分用ラベルなど）を持たせるテーブル
    """
    __tablename__ = "thread_meta"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, unique=True, index=True)
    label = Column(Text, nullable=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================
# BASIC 認証
# =========================
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)


def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
    if not BASIC_ENABLED:
        return
    correct_username = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    correct_password = secrets.compare_digest(credentials.password, BASIC_AUTH_PASS)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


# =========================
# 外部スレッド検索用 定数
# =========================
AREA_OPTIONS = [
    {"code": "",   "label": "地域を選択"},
    {"code": "1",  "label": "北海道版"},
    {"code": "14", "label": "北東北版（青森・岩手・秋田）"},
    {"code": "2",  "label": "南東北版（宮城・山形・福島）"},
    {"code": "15", "label": "北関東版（茨城・栃木・群馬）"},
    {"code": "3",  "label": "南関東・東京周辺版"},
    {"code": "4",  "label": "甲信越版（新潟・長野・山梨）"},
    {"code": "6",  "label": "北陸版（富山・石川・福井）"},
    {"code": "5",  "label": "東海版（愛知・岐阜・静岡・三重）"},
    {"code": "18", "label": "関西版（滋賀・京都・兵庫・奈良・和歌山）"},
    {"code": "7",  "label": "大阪版（大阪単独）"},
    {"code": "8",  "label": "山陽版（岡山・広島・山口）"},
    {"code": "12", "label": "山陰版（鳥取・島根）"},
    {"code": "9",  "label": "四国版（徳島・香川・愛媛・高知）"},
    {"code": "10", "label": "北部九州版（福岡・佐賀・長崎・大分）"},
    {"code": "16", "label": "南部九州版（熊本・宮崎・鹿児島）"},
    {"code": "11", "label": "沖縄版"},
]

BOARD_CATEGORY_OPTIONS = [
    {"id": "",      "label": "（カテゴリ指定なし）"},
    {"id": "103",   "label": "風俗掲示板"},
    {"id": "136",   "label": "メンエス・リフレ・癒し掲示板"},
    {"id": "122",   "label": "R18掲示板"},
]

# 板マスタ（地域別・カテゴリ別）
BOARD_MASTER: Dict[str, List[Dict[str, str]]] = {
    # ctgid=103 風俗掲示板
    "103": [
        # 北海道ブロック acode=1
        {"id": "439",  "label": "北海道風俗・お店"},
        {"id": "2508", "label": "北海道風俗・個人"},
        {"id": "2626", "label": "北海道風俗・総合"},

        {"id": "442",  "label": "札幌風俗・お店"},
        {"id": "1650", "label": "札幌風俗・個人"},
        {"id": "2509", "label": "札幌風俗・総合"},

        {"id": "2012", "label": "函館風俗・お店"},
        {"id": "2013", "label": "函館風俗・個人"},
        {"id": "2677", "label": "函館風俗・総合"},

        # 関西ブロック（滋賀・京都・兵庫）acode=18
        # 滋賀
        {"id": "239",  "label": "滋賀風俗・お店"},
        {"id": "1018", "label": "滋賀風俗・個人"},
        {"id": "1570", "label": "滋賀風俗・総合"},

        # 京都
        {"id": "103",  "label": "京都風俗・お店"},
        {"id": "1228", "label": "京都風俗・個人"},
        {"id": "1614", "label": "京都風俗・総合"},

        # 兵庫
        {"id": "436",  "label": "兵庫風俗・お店"},
        {"id": "975",  "label": "兵庫風俗・個人"},
        {"id": "1141", "label": "兵庫風俗・総合"},

        # 大阪（関西カテゴリでは無く独立）acode=7
        {"id": "410",  "label": "大阪風俗・お店"},
        {"id": "1227", "label": "大阪風俗・個人"},
        {"id": "332",  "label": "大阪風俗・総合"},
        {"id": "5922", "label": "大阪デリヘル・お店"},
        {"id": "5923", "label": "大阪デリヘル・個人"},
        {"id": "5924", "label": "大阪デリヘル・総合"},
        {"id": "3913", "label": "大阪遊郭・新地お店"},
        {"id": "5384", "label": "大阪遊郭・新地個人"},
        {"id": "3392", "label": "大阪遊郭・新地総合"},

        # 愛知 acode=5
        {"id": "472",  "label": "愛知風俗・お店"},
        {"id": "1229", "label": "愛知風俗・個人"},
        {"id": "1302", "label": "愛知風俗・総合"},

        # 東京 acode=3
        {"id": "412",  "label": "東京風俗・お店"},
        {"id": "2020", "label": "東京風俗・個人"},
        {"id": "2026", "label": "東京風俗・総合"},
        {"id": "5919", "label": "東京デリヘル・お店"},
        {"id": "5920", "label": "東京デリヘル・個人"},
        {"id": "5921", "label": "東京デリヘル・総合"},

        {"id": "5869", "label": "東京外国人風俗・お店"},
        {"id": "5870", "label": "東京外国人風俗・総合"},
        {"id": "5072", "label": "吉原ソープ・お店"},
        {"id": "5074", "label": "吉原ソープ・個人"},
        {"id": "5073", "label": "吉原ソープ・総合"},
        {"id": "2277", "label": "西東京風俗・お店"},
        {"id": "2278", "label": "西東京風俗・個人"},
        {"id": "2640", "label": "西東京風俗・総合"},

        # 福岡 acode=10
        {"id": "447",  "label": "福岡風俗・お店"},
        {"id": "984",  "label": "福岡風俗・個人"},
        {"id": "1118", "label": "福岡風俗・総合"},
    ],

    # ctgid=136 メンエス・リフレ・癒し掲示板　acodeは上記参照
    "136": [
        # 北海道ブロック
        {"id": "2887", "label": "北海道メンエス・リフレ・癒し・お店"},
        {"id": "4924", "label": "北海道メンエス・リフレ・癒し・個人"},
        {"id": "3342", "label": "北海道メンエス・リフレ・癒し・総合"},

        # 関西ブロック（滋賀・京都・兵庫）
        # 滋賀
        {"id": "2452", "label": "滋賀メンエス・リフレ・癒し・お店"},
        {"id": "4884", "label": "滋賀メンエス・リフレ・癒し・個人"},
        {"id": "3344", "label": "滋賀メンエス・リフレ・癒し・総合"},

        # 京都
        {"id": "2168", "label": "京都メンエス・リフレ・癒し・お店"},
        {"id": "4886", "label": "京都メンエス・リフレ・癒し・個人"},
        {"id": "2517", "label": "京都メンエス・リフレ・癒し・総合"},

        # 兵庫
        {"id": "1298", "label": "兵庫メンエス・リフレ・癒し・お店"},
        {"id": "4888", "label": "兵庫メンエス・リフレ・癒し・個人"},
        {"id": "2382", "label": "兵庫メンエス・リフレ・癒し・総合"},

        # 大阪（関西カテゴリでは無く独立）
        {"id": "1714", "label": "大阪メンエス・リフレ・癒し・お店"},
        {"id": "4887", "label": "大阪メンエス・リフレ・癒し・個人"},
        {"id": "2383", "label": "大阪メンエス・リフレ・癒し・総合"},
        {"id": "5878", "label": "大阪外国人メンエス・リフレ・癒し・お店"},
        {"id": "5882", "label": "大阪外国人メンエス・リフレ・癒し・総合"},

        # 愛知
        {"id": "1326", "label": "愛知メンエス・リフレ・癒し・お店"},
        {"id": "4880", "label": "愛知メンエス・リフレ・癒し・個人"},
        {"id": "2375", "label": "愛知メンエス・リフレ・癒し・総合"},
        {"id": "5874", "label": "愛知外国人メンエス・リフレ・癒し・お店"},
        {"id": "5875", "label": "愛知外国人メンエス・リフレ・癒し・総合"},

        # 東京
        {"id": "2027", "label": "東京メンエス・リラクゼーション・お店"},
        {"id": "4876", "label": "東京メンエス・リラクゼーション・個人"},
        {"id": "2389", "label": "東京メンエス・リラクゼーション・総合"},
        {"id": "5871", "label": "東京外国人メンエス・リフレ・癒し・お店"},
        {"id": "5872", "label": "東京外国人メンエス・リフレ・癒し・総合"},

        {"id": "5060", "label": "西東京メンエス・リフレ・癒し・お店"},
        {"id": "5061", "label": "西東京メンエス・リフレ・癒し・個人"},
        {"id": "5062", "label": "西東京メンエス・リフレ・癒し・総合"},
        {"id": "5888", "label": "東京リフレ・癒し・お店"},
        {"id": "5889", "label": "東京リフレ・癒し・総合"},

        # 福岡
        {"id": "1331", "label": "福岡メンエス・リフレ・癒し・お店"},
        {"id": "4899", "label": "福岡メンエス・リフレ・癒し・個人"},
        {"id": "2367", "label": "福岡メンエス・リフレ・癒し・総合"},
    ],
    # ctgid=122 R18掲示板 全地域共通なので地域に限らず出す
    "122": [
        {"id": "61",   "label": "Hな悩み"},
        {"id": "508",  "label": "チョットHな雑談"},
        {"id": "1804", "label": "ライブチャット"},
    ],
}

PERIOD_OPTIONS = [
    {"id": "all", "label": "すべて", "days": None},
    {"id": "7d", "label": "7日以内", "days": 7},
    {"id": "1m", "label": "1ヶ月以内", "days": 31},
    {"id": "3m", "label": "3ヶ月以内", "days": 93},
    {"id": "6m", "label": "6ヶ月以内", "days": 186},
    {"id": "1y", "label": "1年以内", "days": 365},
    {"id": "2y", "label": "2年以内", "days": 730},
]

PERIOD_ID_TO_DAYS: Dict[str, Optional[int]] = {
    p["id"]: p["days"] for p in PERIOD_OPTIONS
}


def get_period_days(period_id: str) -> Optional[int]:
    return PERIOD_ID_TO_DAYS.get(period_id)


def get_board_options_for_category(board_category_id: str) -> List[Dict[str, str]]:
    """
    板カテゴリID（ctgid）から、あらかじめ定義した板一覧を返す
    """
    board_category_id = (board_category_id or "").strip()
    return BOARD_MASTER.get(board_category_id, [])


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
        # 先頭側の空白行は丸ごと削る
        if leading and line.strip() == "":
            continue
        # 行頭の半角/全角スペースを削る
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


def normalize_for_search(s: Optional[str]) -> str:
    """
    検索用の正規化：
    - NFKC
    - カタカナ → ひらがな
    - 小文字化
    """
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = to_hiragana(s)
    s = s.lower()
    return s


def _build_highlight_variants(keyword: str) -> List[str]:
    """
    強調表示のときに、ひらがな/カタカナ両対応でマッチさせるためのバリアント生成
    """
    if not keyword:
        return []
    base = unicodedata.normalize("NFKC", keyword)
    hira = to_hiragana(base)
    kata = to_katakana(hira)
    variants = {base, hira, kata}
    variants = {v for v in variants if v}
    return sorted(variants, key=len, reverse=True)


def highlight_text(text_value: Optional[str], keyword: str) -> Markup:
    """
    本文の中でキーワード部分を <mark> で囲って強調表示
    （ひらがな/カタカナ両方ヒット）
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


def build_reply_tree(all_posts: List["ThreadPost"], root: "ThreadPost") -> List[dict]:
    """
    root（ヒットしたレス）にぶら下がる返信ツリーを構築
    """
    replies: Dict[int, List[ThreadPost]] = defaultdict(list)
    for p in all_posts:
        for a in parse_anchors_csv(p.anchors):
            replies[a].append(p)

    result: List[dict] = []
    visited_ids: set[int] = set()

    def dfs(post: ThreadPost, depth: int) -> None:
        if post.id in visited_ids:
            return
        visited_ids.add(post.id)
        if post.id != root.id:
            result.append({"post": post, "depth": depth})
        if post.post_no is None:
            return
        for child in replies.get(post.post_no, []):
            dfs(child, depth + 1)

    if root.post_no is not None:
        for child in replies.get(root.post_no, []):
            dfs(child, 0)
    return result


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


# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,   # /docs を封印
    redoc_url=None,  # /redoc も封印
)
templates = Jinja2Templates(directory="templates")

# 静的ファイル（CSS 等）
app.mount("/static", StaticFiles(directory="static"), name="static")  # ★追加

# 最近の検索条件（メモリ上）
RECENT_SEARCHES = deque(maxlen=5)
EXTERNAL_SEARCHES = deque(maxlen=15)


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    # 既存テーブルに列追加（なければ）
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS tags TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS memo TEXT"))
        conn.execute(text("ALTER TABLE thread_posts ADD COLUMN IF NOT EXISTS thread_title TEXT"))


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"


# =========================
# スレ取り込み共通処理
# =========================
def fetch_thread_into_db(db: Session, url: str) -> int:
    url = (url or "").strip()
    if not url:
        return 0

    last_no = (
        db.query(func.max(ThreadPost.post_no))
        .filter(ThreadPost.thread_url == url)
        .scalar()
    )
    if last_no is None:
        last_no = 0

    # タイトル取得・簡略化
    thread_title = get_thread_title(url)
    if thread_title:
        thread_title = simplify_thread_title(thread_title)
        db.query(ThreadPost).filter(
            ThreadPost.thread_url == url,
            ThreadPost.thread_title.is_(None),
        ).update(
            {ThreadPost.thread_title: thread_title},
            synchronize_session=False,
        )

    scraped_posts = fetch_posts_from_thread(url)
    count = 0
    for sp in scraped_posts:
        body = (sp.body or "").strip()
        if not body:
            continue
        if sp.post_no is not None and sp.post_no <= last_no:
            # すでに取り込み済みのレスはスキップ
            continue

        if getattr(sp, "anchors", None):
            anchors_str = "," + ",".join(str(a) for a in sp.anchors) + ","
        else:
            anchors_str = None

        # すでに同じレスが入っていれば更新のみ
        if sp.post_no is not None:
            existing = (
                db.query(ThreadPost)
                .filter(
                    ThreadPost.thread_url == url,
                    ThreadPost.post_no == sp.post_no,
                )
                .first()
            )
        else:
            existing = (
                db.query(ThreadPost)
                .filter(
                    ThreadPost.thread_url == url,
                    ThreadPost.body == body,
                )
                .first()
            )

        if existing:
            if not existing.posted_at and getattr(sp, "posted_at", None):
                existing.posted_at = sp.posted_at
            if not existing.anchors and anchors_str:
                existing.anchors = anchors_str
            if thread_title and not existing.thread_title:
                existing.thread_title = thread_title
            continue

        db.add(
            ThreadPost(
                thread_url=url,
                thread_title=thread_title,
                post_no=sp.post_no,
                posted_at=getattr(sp, "posted_at", None),  # ★ここを修正
                body=body,
                anchors=anchors_str,
            )
        )
        count += 1

    db.commit()
    return count


# =========================
# トップページ（内部検索）
# =========================
@app.get("/", response_class=HTMLResponse)
def show_search_page(
    request: Request,
    q: str = "",
    thread_filter: str = "",
    tags: str = "",
    tag_mode: str = "or",
    db: Session = Depends(get_db),
):
    keyword_raw = (q or "").strip()
    thread_filter_raw = (thread_filter or "").strip()
    tags_input_raw = (tags or "").strip()
    tag_mode = (tag_mode or "or").lower()

    keyword_norm = normalize_for_search(keyword_raw)
    thread_filter_norm = normalize_for_search(thread_filter_raw)

    tags_norm_list: List[str] = []
    if tags_input_raw:
        tags_norm_list = [
            normalize_for_search(t)
            for t in tags_input_raw.split(",")
            if t.strip()
        ]

    thread_results: List[dict] = []
    hit_count = 0
    error_message: str = ""
    popular_tags: List[dict] = []
    recent_searches_view: List[dict] = []

    try:
        # 全タグの集計（タグ一覧用）
        tag_rows = db.query(ThreadPost.tags).filter(ThreadPost.tags.isnot(None)).all()
        tag_counts: Dict[str, int] = {}
        for (tags_str,) in tag_rows:
            if not tags_str:
                continue
            for tag in tags_str.split(","):
                tag = tag.strip()
                if not tag:
                    continue
                tag_counts[tag] = tag_counts.get(tag, 0) + 1

        popular_tags = [
            {"name": name, "count": count}
            for name, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50]
        ]

        # 検索条件が入力されている場合のみ検索を実行
        if keyword_raw or thread_filter_raw or tags_input_raw:
            # 最近の検索条件をメモリに保存
            params = {
                "q": keyword_raw,
                "thread_filter": thread_filter_raw,
                "tags": tags_input_raw,
                "tag_mode": tag_mode,
            }
            qs = urlencode(params, doseq=False)
            entry = {"params": params, "url": "/?" + qs}
            if not any(e["url"] == entry["url"] for e in RECENT_SEARCHES):
                RECENT_SEARCHES.append(entry)

            all_posts: List[ThreadPost] = (
                db.query(ThreadPost)
                .order_by(ThreadPost.thread_url.asc(), ThreadPost.post_no.asc())
                .all()
            )

            posts_by_thread: Dict[str, List[ThreadPost]] = defaultdict(list)
            for p in all_posts:
                posts_by_thread[p.thread_url].append(p)

            hits: List[ThreadPost] = []
            for p in all_posts:
                body_norm = normalize_for_search(p.body or "")
                if keyword_norm and keyword_norm not in body_norm:
                    continue

                if thread_filter_norm:
                    url_norm = normalize_for_search(p.thread_url or "")
                    title_norm = normalize_for_search(p.thread_title or "")
                    if thread_filter_norm not in url_norm and thread_filter_norm not in title_norm:
                        continue

                if tags_norm_list:
                    post_tags_norm = normalize_for_search(p.tags or "")
                    if tag_mode == "and":
                        ok = all(t in post_tags_norm for t in tags_norm_list)
                    else:
                        ok = any(t in post_tags_norm for t in tags_norm_list)
                    if not ok:
                        continue

                hits.append(p)

            hit_count = len(hits)

            if hits:
                thread_map: Dict[str, dict] = {}
                for root in hits:
                    thread_url = root.thread_url
                    block = thread_map.get(thread_url)
                    if not block:
                        title = root.thread_title or thread_url
                        title = simplify_thread_title(title)
                        block = {
                            "thread_url": thread_url,
                            "thread_title": title,
                            "entries": [],
                        }
                        thread_map[thread_url] = block
                        thread_results.append(block)

                    all_posts_thread = posts_by_thread.get(thread_url, [])

                    # 前後 5 レスのコンテキスト
                    context_posts: List[ThreadPost] = []
                    if root.post_no is not None and all_posts_thread:
                        start_no = max(1, root.post_no - 5)
                        end_no = root.post_no + 5
                        context_posts = [
                            p
                            for p in all_posts_thread
                            if p.post_no is not None and start_no <= p.post_no <= end_no
                        ]

                    # ツリー表示用
                    tree_items = build_reply_tree(all_posts_thread, root)

                    # アンカー先（このレスが参照しているレスを再帰的にたどる）

                    # スレ内の post_no → 投稿オブジェクト のマップを作成
                    posts_by_no: Dict[int, ThreadPost] = {}
                    for p2 in all_posts_thread:
                        if p2.post_no is not None and p2.post_no not in posts_by_no:
                            posts_by_no[p2.post_no] = p2

                    def collect_anchor_chain(root_post: ThreadPost) -> List[ThreadPost]:
                        """
                        root_post が参照しているレス → さらにその参照先 … を
                        「参照が途切れるまで」たどって 1 本のリストにする。
                        同じレスをぐるぐる回らないように visited で保護。
                        """
                        result: List[ThreadPost] = []
                        visited: set[int] = set()

                        def dfs(post: ThreadPost) -> None:
                            if post.post_no is None:
                                return
                            if post.post_no in visited:
                                return
                            visited.add(post.post_no)

                            # 自分自身（root_post）は result に入れない
                            if post is not root_post:
                                result.append(post)

                            if not post.anchors:
                                return

                            nums = parse_anchors_csv(post.anchors)
                            for n in nums:
                                target = posts_by_no.get(n)
                                if target:
                                    dfs(target)

                        if root_post.anchors:
                            nums = parse_anchors_csv(root_post.anchors)
                            for n in nums:
                                target = posts_by_no.get(n)
                                if target:
                                    dfs(target)

                        return result

                    anchor_targets = collect_anchor_chain(root)

                    block["entries"].append(
                        {
                            "root": root,
                            "context": context_posts,
                            "tree": tree_items,
                            "anchor_targets": anchor_targets,
                        }
                    )


    except Exception as e:
        db.rollback()
        error_message = f"検索中にエラーが発生しました: {e}"
        thread_results = []
        hit_count = 0

    recent_searches_view = list(RECENT_SEARCHES)[::-1]


    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyword": keyword_raw,
            "thread_filter": thread_filter_raw,
            "tags_input": tags_input_raw,
            "tag_mode": tag_mode,
            "results": thread_results,
            "hit_count": hit_count,
            "highlight": highlight_text,
            "error_message": error_message,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
        },
    )


# =========================
# JSON API（簡易）
# =========================
@app.get("/api/search")
def api_search(
    q: str,
    thread_filter: str = "",
    db: Session = Depends(get_db),
):
    keyword = (q or "").strip()
    thread_filter = (thread_filter or "").strip()
    if not keyword:
        return []

    query = db.query(ThreadPost).filter(ThreadPost.body.contains(keyword))
    if thread_filter:
        query = query.filter(ThreadPost.thread_url.contains(thread_filter))

    posts = query.order_by(ThreadPost.id.asc()).all()
    return [
        {
            "id": p.id,
            "thread_url": p.thread_url,
            "thread_title": p.thread_title,
            "post_no": p.post_no,
            "posted_at": p.posted_at,
            "body": p.body,
            "anchors": p.anchors,
            "tags": p.tags,
            "memo": p.memo,
        }
        for p in posts
    ]


# =========================
# 管理用：スレ取り込み画面
# =========================
@app.get("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_get(request: Request, url: str = ""):
    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": url or "",
            "imported": None,
            "error": "",
        },
    )


@app.post("/admin/fetch", response_class=HTMLResponse)
def fetch_thread_post(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    imported: Optional[int] = None
    error: str = ""
    url = (url or "").strip()
    if url:
        try:
            imported = fetch_thread_into_db(db, url)
        except ScrapingError as e:
            db.rollback()
            error = str(e)
        except Exception as e:
            db.rollback()
            error = f"想定外のエラーが発生しました: {e}"
    else:
        error = "URLが入力されていません。"

    return templates.TemplateResponse(
        "fetch.html",
        {
            "request": request,
            "url": url,
            "imported": imported,
            "error": error,
        },
    )


@app.post("/admin/refetch")
def refetch_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@app.post("/admin/delete_thread")
def delete_thread_from_search(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/"
    url = (url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        db.query(ThreadPost).filter(ThreadPost.thread_url == url).delete(
            synchronize_session=False
        )
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


# =========================
# スレッド一覧ダッシュボード (/threads)
# =========================
@app.get("/threads", response_class=HTMLResponse)
def list_threads(
    request: Request,
    db: Session = Depends(get_db),
):
    # スレ単位の集約
    rows = (
        db.query(
            ThreadPost.thread_url,
            func.max(ThreadPost.post_no).label("max_no"),
            func.max(ThreadPost.posted_at).label("last_posted_at"),
            func.min(ThreadPost.thread_title).label("thread_title"),
            func.count().label("post_count"),
        )
        .group_by(ThreadPost.thread_url)
        .order_by(func.max(ThreadPost.posted_at).desc())
        .all()
    )

    urls = [r.thread_url for r in rows]
    meta_map: Dict[str, ThreadMeta] = {}
    if urls:
        metas = db.query(ThreadMeta).filter(ThreadMeta.thread_url.in_(urls)).all()
        meta_map = {m.thread_url: m for m in metas}

    threads = []
    for r in rows:
        label = None
        if r.thread_url in meta_map:
            label = meta_map[r.thread_url].label
        threads.append(
            {
                "thread_url": r.thread_url,
                "thread_title": simplify_thread_title(r.thread_title or r.thread_url),
                "max_no": r.max_no,
                "last_posted_at": r.last_posted_at,
                "post_count": r.post_count,
                "label": label or "",
            }
        )

    # タグ一覧
    tag_rows = db.query(ThreadPost.tags).filter(ThreadPost.tags.isnot(None)).all()
    tag_counts: Dict[str, int] = {}
    for (tags_str,) in tag_rows:
        if not tags_str:
            continue
        for tag in tags_str.split(","):
            tag = tag.strip()
            if not tag:
                continue
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    popular_tags = [
        {"name": name, "count": count}
        for name, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    ]

    # 最近の検索条件
    recent_searches_view = list(RECENT_SEARCHES)[::-1]

    # 次スレ取得結果メッセージ
    info_message = ""
    try:
        params = request.query_params
        if params.get("next_ok"):
            info_message = "次スレを取り込みました。"
        elif params.get("no_next"):
            info_message = "次スレが見つかりませんでした。"
        elif params.get("next_error"):
            info_message = "次スレ取得中にエラーが発生しました。"
    except Exception:
        info_message = ""

    return templates.TemplateResponse(
        "threads.html",
        {
            "request": request,
            "threads": threads,
            "popular_tags": popular_tags,
            "recent_searches": recent_searches_view,
            "info_message": info_message,
        },
    )


@app.post("/threads/label")
def update_thread_label(
    request: Request,
    thread_url: str = Form(""),
    label: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or "/threads"
    url = (thread_url or "").strip()
    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    label = (label or "").strip()
    try:
        meta = db.query(ThreadMeta).filter(ThreadMeta.thread_url == url).first()
        if not meta:
            meta = ThreadMeta(thread_url=url, label=label or None)
            db.add(meta)
        else:
            meta.label = label or None
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


# =========================
# 投稿単位のタグ・メモ編集
# =========================
@app.get("/post/{post_id}/edit", response_class=HTMLResponse)
def edit_post_get(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
):
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    return templates.TemplateResponse(
        "edit_post.html",
        {
            "request": request,
            "post": post,
        },
    )


@app.post("/post/{post_id}/edit")
def edit_post_post(
    request: Request,
    post_id: int,
    tags: str = Form(""),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    post = db.query(ThreadPost).filter(ThreadPost.id == post_id).first()
    if post:
        post.tags = (tags or "").strip() or None
        post.memo = (memo or "").strip() or None
        db.commit()

    back_url = request.headers.get("referer") or "/"
    return RedirectResponse(url=back_url, status_code=303)


# =========================
# 外部スレッド検索（爆サイ）
# =========================
def search_threads_external(
    area_code: str,
    keyword: str,
    max_days: Optional[int],
    board_category: str = "",
    board_id: str = "",
) -> List[dict]:
    """
    爆サイの「スレッド検索」結果から、タイトル一覧を取得する。
    board_category, board_id が指定されていればそれをURLに反映。
    """
    keyword = (keyword or "").strip()
    area_code = (area_code or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    if not area_code or not keyword:
        return []

    # URL 組み立て
    # 例： https://bakusai.com/sch_thr_thread/acode=7/ctgid=103/bid=410/p=1/sch=thr_sch/sch_range=board/word=ピンク/
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

    # 「最新レス投稿日時」テキストを手掛かりにスレブロックを探す
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
        # 近くの /thr_res/ へのリンクを探す
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

        if href.startswith("//"):
            full_url = "https:" + href
        elif href.startswith("/"):
            full_url = "https://bakusai.com" + href
        else:
            full_url = href

        threads.append(
            {
                "title": title,
                "url": full_url,
                "last_post_at_str": dt.strftime("%Y-%m-%d %H:%M"),
            }
        )

    # URL でユニーク化
    unique_by_url: Dict[str, dict] = {}
    for t in threads:
        if t["url"] not in unique_by_url:
            unique_by_url[t["url"]] = t

    result = list(unique_by_url.values())
    result.sort(key=lambda x: x.get("last_post_at_str") or "", reverse=True)
    return result


@app.get("/thread_search", response_class=HTMLResponse)
def thread_search_page(
    request: Request,
    area: str = "7",
    period: str = "3m",
    keyword: str = "",
    board_category: str = "103",
    board_id: str = "5922",
):
    # パラメータ整形
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"
    keyword = (keyword or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()

    results: List[dict] = []
    error_message = ""

    # ★ ランキング結果（板ごと）
    ranking_board = None
    ranking_board_label = ""
    ranking_source_url = ""

    # 板リストをカテゴリから取得（履歴のラベル用にも使う）
    board_options = get_board_options_for_category(board_category)

    # 検索実行
    if keyword and area:
        max_days = get_period_days(period)
        try:
            results = search_threads_external(
                area_code=area,
                keyword=keyword,
                max_days=max_days,
                board_category=board_category,
                board_id=board_id,
            )
        except Exception as e:
            error_message = f"外部検索中にエラーが発生しました: {e}"

        # ★ ランキング取得（検索が成功していて、板が指定されているときだけ）
        if not error_message and board_category and board_id:
            # 表示用ラベル（「大阪デリヘル・お店」など）を取得
            board_label = ""
            for b in board_options:
                if b["id"] == board_id:
                    board_label = b["label"]
                    break

            ranking_board_label = board_label or "選択した板"
            ranking_board = get_board_ranking(area, board_category, board_id)

            # ★ 爆サイ側のランキング元ページURL（thr_tl）を組み立て
            if ranking_board:
                ranking_source_url = RANKING_URL_TEMPLATE.format(
                    acode=area,
                    ctgid=board_category,
                    bid=board_id,
                )

        # 検索履歴を追加（エラーが出ていないときだけ）
        if not error_message:
            area_label = next(
                (a["label"] for a in AREA_OPTIONS if a["code"] == area),
                area,
            )
            period_label = next(
                (p["label"] for p in PERIOD_OPTIONS if p["id"] == period),
                period,
            )
            if board_category:
                board_category_label = next(
                    (c["label"] for c in BOARD_CATEGORY_OPTIONS if c["id"] == board_category),
                    board_category,
                )
            else:
                board_category_label = "（カテゴリ指定なし）"

            board_label = ""
            if board_category and board_id:
                for b in board_options:
                    if b["id"] == board_id:
                        board_label = b["label"]
                        break

            key = f"{area}|{period}|{board_category}|{board_id}|{keyword}"
            entry = {
                "key": key,
                "area": area,
                "area_label": area_label,
                "period": period,
                "period_label": period_label,
                "board_category": board_category,
                "board_category_label": board_category_label,
                "board_id": board_id,
                "board_label": board_label,
                "keyword": keyword,
            }
            if not any(e["key"] == key for e in EXTERNAL_SEARCHES):
                EXTERNAL_SEARCHES.append(entry)

    recent_external_searches = list(EXTERNAL_SEARCHES)[::-1]

    # スレ保存完了フラグ（/thread_search/save からのリダイレクト時）
    try:
        saved_flag = request.query_params.get("saved")
    except Exception:
        saved_flag = None
    if saved_flag and not error_message:
        error_message = "スレッドを保存しました。"

    return templates.TemplateResponse(
        "thread_search.html",
        {
            "request": request,
            "area_options": AREA_OPTIONS,
            "period_options": PERIOD_OPTIONS,
            "board_category_options": BOARD_CATEGORY_OPTIONS,
            "current_area": area,
            "current_period": period,
            "keyword": keyword,
            "results": results,
            "error_message": error_message,
            "board_options": board_options,
            "current_board_category": board_category,
            "current_board_id": board_id,
            "recent_external_searches": recent_external_searches,
            "board_master_json": json.dumps(BOARD_MASTER, ensure_ascii=False),
            # ★ 追加
            "ranking_board": ranking_board,
            "ranking_board_label": ranking_board_label,
            "ranking_source_url": ranking_source_url,
        },
    )


def _add_flag_to_url(back_url: str, key: str) -> str:
    """
    クエリに {key}=1 を付与するヘルパー。
    すでに付いている場合はそのまま返す。
    """
    if not back_url:
        return f"/thread_search?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


# =========================
# 外部スレッド → DB 保存
# =========================
@app.api_route("/thread_search/save", methods=["GET", "POST"])
async def save_external_thread(
    request: Request,
    db: Session = Depends(get_db),
):
    """
    外部スレッド検索・スレッド内検索結果から
    「このスレを保存」で呼び出されるエンドポイント。

    - GET / POST どちらで来ても対応
    - thread_url / selected_thread のどちらかに URL が入っていれば保存
    """
    back_url = request.headers.get("referer") or "/thread_search"

    # スレ内検索結果（/thread_search/posts）から呼ばれた場合、
    # そのまま戻すと GET /thread_search/posts になり 405 になるので
    # 安全な /thread_search に退避させる。
    if back_url and "/thread_search/posts" in back_url:
        back_url = "/thread_search"

    url = ""
    saved_ok = False

    try:
        if request.method == "POST":
            form = await request.form()
            thread_url = (form.get("thread_url") or "").strip()
            selected_thread = (form.get("selected_thread") or "").strip()
            url = thread_url or selected_thread
        else:
            params = request.query_params
            thread_url = (params.get("thread_url") or "").strip()
            selected_thread = (params.get("selected_thread") or "").strip()
            url = thread_url or selected_thread
    except Exception:
        url = ""

    if not url:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
        saved_ok = True
    except Exception:
        db.rollback()

    if saved_ok:
        redirect_to = _add_flag_to_url(back_url, "saved")
    else:
        redirect_to = back_url

    return RedirectResponse(url=redirect_to, status_code=303)


# =========================
# 次スレ取得（保存スレ・内部検索共通で使える）
# =========================
def _normalize_bakusai_href(href: str) -> str:
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://bakusai.com" + href
    return href


def find_prev_next_thread_urls(thread_url: str, area_code: str) -> tuple[Optional[str], Optional[str]]:
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


@app.post("/admin/fetch_next")
def fetch_next_thread(
    request: Request,
    url: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    取り込み済みスレの「次スレ」を取得して DB に保存する。
    呼び出し元（/threads や /）にリダイレクトし、クエリパラメータで結果を返す。
    """
    back_url = request.headers.get("referer") or "/threads"
    url = (url or "").strip()

    if not url:
        redirect_to = _add_flag_to_url(back_url, "next_error")
        return RedirectResponse(url=redirect_to, status_code=303)

    # ページャーから次スレ URL を取得
    _, next_url = find_prev_next_thread_urls(url, "")
    if not next_url:
        redirect_to = _add_flag_to_url(back_url, "no_next")
        return RedirectResponse(url=redirect_to, status_code=303)

    try:
        fetch_thread_into_db(db, next_url)
        redirect_to = _add_flag_to_url(back_url, "next_ok")
    except Exception:
        db.rollback()
        redirect_to = _add_flag_to_url(back_url, "next_error")

    return RedirectResponse(url=redirect_to, status_code=303)


# =========================
# 外部検索履歴の削除
# =========================
@app.post("/thread_search/history/delete")
def delete_external_history(
    request: Request,
    key: str = Form(""),
):
    back_url = request.headers.get("referer") or "/thread_search"
    key = (key or "").strip()
    if not key:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        remaining = [e for e in EXTERNAL_SEARCHES if e.get("key") != key]
        EXTERNAL_SEARCHES.clear()
        EXTERNAL_SEARCHES.extend(remaining)
    except Exception:
        pass

    return RedirectResponse(url=back_url, status_code=303)


@app.post("/thread_search/history/clear")
def clear_external_history(request: Request):
    back_url = request.headers.get("referer") or "/thread_search"
    try:
        EXTERNAL_SEARCHES.clear()
    except Exception:
        pass
    return RedirectResponse(url=back_url, status_code=303)


# =========================
# 外部スレッド内検索
# =========================
@app.post("/thread_search/posts", response_class=HTMLResponse)
def thread_search_posts(
    request: Request,
    selected_thread: str = Form(""),
    title_keyword: str = Form(""),
    post_keyword: str = Form(""),
    area: str = Form("7"),
    period: str = Form("3m"),
):
    selected_thread = (selected_thread or "").strip()
    title_keyword = (title_keyword or "").strip()
    post_keyword = (post_keyword or "").strip()
    area = (area or "").strip() or "7"
    period = (period or "").strip() or "3m"

    entries: List[dict] = []
    error_message = ""
    thread_title_display: str = ""
    prev_thread_url: Optional[str] = None
    next_thread_url: Optional[str] = None

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    else:
        try:
            try:
                t = get_thread_title(selected_thread)
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread, area)

            all_posts = fetch_posts_from_thread(selected_thread)

            # post_no でソートしておく（コンテキスト順がバラつかないように）
            def _post_key(p):
                return p.post_no if getattr(p, "post_no", None) is not None else 10**9

            all_posts_sorted = sorted(list(all_posts), key=_post_key)

            posts_by_no: Dict[int, object] = {}
            for p in all_posts_sorted:
                if p.post_no is not None and p.post_no not in posts_by_no:
                    posts_by_no[p.post_no] = p

            replies: Dict[int, List[object]] = defaultdict(list)
            for p in all_posts_sorted:
                if not getattr(p, "anchors", None):
                    continue
                for a in p.anchors:
                    replies[a].append(p)

            def build_reply_tree_external(root) -> List[dict]:
                result: List[dict] = []
                visited: set[int] = set()

                def dfs(post, depth: int):
                    pid = id(post)
                    if pid in visited:
                        return
                    visited.add(pid)
                    if post is not root:
                        result.append({"post": post, "depth": depth})
                    if post.post_no is None:
                        return
                    for child in replies.get(post.post_no, []):
                        dfs(child, depth + 1)

                if root.post_no is not None:
                    for child in replies.get(root.post_no, []):
                        dfs(child, 0)
                return result

            post_keyword_norm = normalize_for_search(post_keyword)

            for root in all_posts_sorted:
                body = root.body or ""
                body_norm = normalize_for_search(body)
                if post_keyword_norm not in body_norm:
                    continue

                context_posts: List[object] = []
                if root.post_no is not None:
                    start_no = max(1, root.post_no - 5)
                    end_no = root.post_no + 5
                    for p in all_posts_sorted:
                        if p.post_no is None:
                            continue
                        if start_no <= p.post_no <= end_no:
                            context_posts.append(p)

                tree_items = build_reply_tree_external(root)

            # このレスが参照しているレス（再帰的にたどる）
            def collect_anchor_chain_external(root_post) -> List[object]:
                """
                root_post → anchors で参照しているレス → さらにその参照先…
                を再帰的にたどって 1 本のリストにする。
                """
                result: List[object] = []
                visited: set[int] = set()

                def dfs(post) -> None:
                    if post.post_no is None:
                        return
                    if post.post_no in visited:
                        return
                    visited.add(post.post_no)

                    if post is not root_post:
                        result.append(post)

                    if not getattr(post, "anchors", None):
                        return

                    for n in post.anchors:
                        target = posts_by_no.get(n)
                        if target:
                            dfs(target)

                if getattr(root_post, "anchors", None):
                    for n in root_post.anchors:
                        target = posts_by_no.get(n)
                        if target:
                            dfs(target)

                return result

            anchor_targets = collect_anchor_chain_external(root)

            entries.append(
                {
                    "root": root,
                    "context": context_posts,
                    "tree": tree_items,
                    "anchor_targets": anchor_targets,
                }
            )


        except Exception as e:
            error_message = f"スレッド内検索中にエラーが発生しました: {e}"
            entries = []

    return templates.TemplateResponse(
        "thread_search_posts.html",
        {
            "request": request,
            "thread_url": selected_thread,
            "thread_title": thread_title_display,
            "title_keyword": title_keyword,
            "post_keyword": post_keyword,
            "area": area,
            "period": period,
            "entries": entries,
            "error_message": error_message,
            "highlight": highlight_text,
            "prev_thread_url": prev_thread_url,
            "next_thread_url": next_thread_url,
        },
    )
