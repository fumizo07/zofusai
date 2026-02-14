# 005
# routers/external_search.py
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Dict
from urllib.parse import urlencode

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app_context import templates
from constants import (
    AREA_OPTIONS,
    BOARD_CATEGORY_OPTIONS,
    BOARD_MASTER,
    PERIOD_OPTIONS,
    get_period_days,
    get_board_options_for_category,
)
from db import get_db
from models import ExternalSearchHistory, ThreadMeta
from services import (
    search_threads_external,
    get_thread_posts_cached,
    find_prev_next_thread_urls,
    fetch_thread_into_db,
    is_valid_bakusai_thread_url,
)
from scraper import get_thread_title
from ranking import get_board_ranking, RANKING_URL_TEMPLATE
from utils import (
    normalize_for_search,
    highlight_text,
    simplify_thread_title,
    parse_anchors_csv,
    highlight_with_links,
    build_store_search_title,
    build_google_site_search_url,
)

# フェーズ1ログ用
import time

router = APIRouter()

# -------------------------
# defaults（ここで統一）
# -------------------------
DEFAULT_AREA = "7"
DEFAULT_PERIOD = "3m"
DEFAULT_BOARD_CATEGORY = "103"
DEFAULT_BOARD_ID_BY_CATEGORY = {
    "103": "5922",  # 大阪デリヘル・お店
    "136": "1714",  # 大阪メンエス…・お店（あなたのマスタ内にある）
    "122": "61",    # Hな悩み
}


def _truthy(v: Optional[str]) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "on")


def _safe_back_url(back_url: str, default: str = "/thread_search") -> str:
    """
    open redirect 対策：アプリ内相対パスのみ許可
    """
    if not back_url:
        return default
    b = (back_url or "").strip()
    if not b.startswith("/"):
        return default
    if b.startswith("//"):
        return default
    return b


def _is_valid_area(area: str) -> bool:
    return any(a["code"] == area for a in AREA_OPTIONS if a.get("code") is not None)


def _is_valid_period(period: str) -> bool:
    return any(p["id"] == period for p in PERIOD_OPTIONS if p.get("id") is not None)


def _is_valid_board_category(board_category: str) -> bool:
    if any(c["id"] == board_category for c in BOARD_CATEGORY_OPTIONS if c.get("id") is not None):
        return True
    # 念のため BOARD_MASTER キーも許可
    return board_category in BOARD_MASTER


def _normalize_thread_search_params(
    area: str,
    period: str,
    board_category: str,
    board_id: str,
    keyword: str,
) -> tuple[str, str, str, str, str]:
    area = (area or "").strip()
    period = (period or "").strip()
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()
    keyword = (keyword or "").strip()

    # area
    if not area or not _is_valid_area(area) or area == "":
        area = DEFAULT_AREA

    # period
    if not period or not _is_valid_period(period):
        period = DEFAULT_PERIOD

    # board_category
    if not board_category or not _is_valid_board_category(board_category):
        board_category = DEFAULT_BOARD_CATEGORY

    # board_id
    options = get_board_options_for_category(board_category)
    if board_id and any(b["id"] == board_id for b in options):
        pass
    else:
        preferred = DEFAULT_BOARD_ID_BY_CATEGORY.get(board_category, "")
        if preferred and any(b["id"] == preferred for b in options):
            board_id = preferred
        else:
            board_id = options[0]["id"] if options else ""

    return area, period, board_category, board_id, keyword


def _build_thread_search_url(
    area: str,
    period: str,
    board_category: str,
    board_id: str,
    keyword: str,
) -> str:
    """
    「戻る」専用URL（no_log はテンプレに書かせず、ルータで付与）
    """
    params = {
        "area": area or "",
        "period": period or "",
        "board_category": board_category or "",
        "board_id": board_id or "",
        "keyword": keyword or "",
        "no_log": "1",  # ★テンプレから隠蔽：ルータ側だけで制御
    }
    return "/thread_search?" + urlencode(params, doseq=False)


def _add_flag_to_url(back_url: str, key: str) -> str:
    if not back_url:
        return f"/?{key}=1"
    if f"{key}=" in back_url:
        return back_url
    if "?" in back_url:
        return back_url + f"&{key}=1"
    return back_url + f"?{key}=1"


def _history_key(area: str, period: str, board_category: str, board_id: str, keyword: str) -> str:
    return f"{area}|{period}|{board_category}|{board_id}|{keyword}"


def _touch_external_history(
    db: Session,
    area: str,
    period: str,
    board_category: str,
    board_id: str,
    keyword: str,
) -> None:
    """
    no_log=0 のときだけ呼ぶ
    - 同じ条件なら 1件に集約し、last_seen_at と hit_count を更新
    """
    key = _history_key(area, period, board_category, board_id, keyword)
    now = datetime.utcnow()

    row = db.query(ExternalSearchHistory).filter(ExternalSearchHistory.key == key).one_or_none()
    if row:
        row.last_seen_at = now
        row.hit_count = (row.hit_count or 0) + 1
    else:
        row = ExternalSearchHistory(
            key=key,
            area=area,
            period=period,
            board_category=board_category,
            board_id=board_id,
            keyword=keyword,
            created_at=now,
            last_seen_at=now,
            hit_count=1,
        )
        db.add(row)

    try:
        db.commit()
    except Exception:
        db.rollback()
        # 競合（同時リクエスト）等で unique 失敗した場合も、ここでは黙って戻す
        # 次回アクセス時に更新されます。


def _build_recent_external_searches(db: Session, limit: int = 15) -> List[dict]:
    rows = (
        db.query(ExternalSearchHistory)
        .order_by(desc(ExternalSearchHistory.last_seen_at))
        .limit(limit)
        .all()
    )

    out: List[dict] = []
    for r in rows:
        area = r.area or ""
        period = r.period or ""
        board_category = r.board_category or ""
        board_id = r.board_id or ""
        keyword = r.keyword or ""

        area_label = next((a["label"] for a in AREA_OPTIONS if a["code"] == area), area)
        period_label = next((p["label"] for p in PERIOD_OPTIONS if p["id"] == period), period)

        if board_category:
            board_category_label = next(
                (c["label"] for c in BOARD_CATEGORY_OPTIONS if c["id"] == board_category),
                board_category,
            )
        else:
            board_category_label = "（カテゴリ指定なし）"

        board_label = ""
        if board_category and board_id:
            for b in get_board_options_for_category(board_category):
                if b["id"] == board_id:
                    board_label = b["label"]
                    break

        out.append(
            {
                "key": r.key,
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
        )
    return out


# =========================
# ★追加（003の機能移植）：スレタイのDBキャッシュ
# =========================
def _get_thread_title_cached(db: Session, url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""

    row = None
    try:
        row = db.query(ThreadMeta).filter(ThreadMeta.thread_url == url).one_or_none()
        if row and (row.label or "").strip():
            return (row.label or "").strip()
    except Exception:
        row = None

    title = ""
    try:
        t = get_thread_title(url)
        title = simplify_thread_title(t or "") or ""
    except Exception:
        title = ""

    title = (title or "").strip()
    if not title:
        return ""

    # upsert っぽく保存（競合してもOK）
    try:
        if row:
            row.label = title
        else:
            db.add(ThreadMeta(thread_url=url, label=title))
        db.commit()
    except Exception:
        db.rollback()
        # 競合の可能性があるので再取得して返す
        try:
            row2 = db.query(ThreadMeta).filter(ThreadMeta.thread_url == url).one_or_none()
            if row2 and (row2.label or "").strip():
                return (row2.label or "").strip()
        except Exception:
            pass

    return title


# =========================
# ★追加：KB経由だけ「板ゆらぎ」フォールバック
# =========================
def _get_board_label(board_category: str, board_id: str) -> str:
    board_category = (board_category or "").strip()
    board_id = (board_id or "").strip()
    if not board_category or not board_id:
        return ""
    for b in get_board_options_for_category(board_category):
        if (b.get("id") or "") == board_id:
            return (b.get("label") or "").strip()
    return ""


def _find_board_id_by_label(board_category: str, label: str) -> str:
    board_category = (board_category or "").strip()
    label = (label or "").strip()
    if not board_category or not label:
        return ""
    for b in get_board_options_for_category(board_category):
        if (b.get("label") or "").strip() == label:
            return (b.get("id") or "").strip()
    return ""


def _fallback_board(board_category: str, board_id: str) -> tuple[str, str]:
    """
    現在の板ラベルから「風俗 ⇄ デリヘル」を入れ替えた板を探す。
    例：
      大阪デリヘル・お店 -> 大阪風俗・お店
      東京風俗・お店     -> 東京デリヘル・お店
    戻り値： (fallback_board_id, fallback_board_label)
    """
    cur_label = _get_board_label(board_category, board_id)
    if not cur_label:
        return "", ""

    # 「風俗」と「デリヘル」のみを対象（それ以外は何もしない）
    if "デリヘル" in cur_label:
        fb_label = cur_label.replace("デリヘル", "風俗")
    elif ("風俗" in cur_label) and ("デリヘル" not in cur_label):
        fb_label = cur_label.replace("風俗", "デリヘル")
    else:
        return "", ""

    fb_id = _find_board_id_by_label(board_category, fb_label)
    if not fb_id:
        return "", ""
    return fb_id, fb_label


@router.post("/thread_search/history/delete")
def delete_external_search_history(
    key: str = Form(""),
    db: Session = Depends(get_db),
):
    key = (key or "").strip()
    if key:
        try:
            db.query(ExternalSearchHistory).filter(ExternalSearchHistory.key == key).delete()
            db.commit()
        except Exception:
            db.rollback()
    return RedirectResponse(url="/thread_search", status_code=303)


@router.post("/thread_search/history/clear")
def clear_external_search_history(
    db: Session = Depends(get_db),
):
    try:
        db.query(ExternalSearchHistory).delete()
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url="/thread_search", status_code=303)


@router.get("/thread_search", response_class=HTMLResponse)
def thread_search_page(
    request: Request,
    area: str = DEFAULT_AREA,
    period: str = DEFAULT_PERIOD,
    keyword: str = "",
    board_category: str = DEFAULT_BOARD_CATEGORY,
    board_id: str = DEFAULT_BOARD_ID_BY_CATEGORY.get(DEFAULT_BOARD_CATEGORY, ""),
    no_log: str = "",
    db: Session = Depends(get_db),
):
    area, period, board_category, board_id, keyword = _normalize_thread_search_params(
        area, period, board_category, board_id, keyword
    )

    # ★no_log はテンプレから隠蔽：戻りURLに勝手に付ける
    no_log_flag = _truthy(no_log) or _truthy(request.query_params.get("no_log"))

    # ★KB経由フラグ（KBからだけ付ける想定）
    kb_flag = _truthy(request.query_params.get("kb"))

    # ★KB経由は「最近の外部検索」に残さない
    if kb_flag:
        no_log_flag = True

    results: List[dict] = []
    error_message = ""

    ranking_board = None
    ranking_board_label = ""
    ranking_source_url = ""

    board_options = get_board_options_for_category(board_category)

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

        # ★KB経由だけ：0件なら「風俗⇄デリヘル」をフォールバック
        if (not error_message) and kb_flag and (not results):
            fb_id, fb_label = _fallback_board(board_category, board_id)
            if fb_id and fb_id != board_id:
                cur_label = _get_board_label(board_category, board_id)
                try:
                    fb_results = search_threads_external(
                        area_code=area,
                        keyword=keyword,
                        max_days=max_days,
                        board_category=board_category,
                        board_id=fb_id,
                    )
                    if fb_results:
                        results = fb_results
                        board_id = fb_id  # ★以後の表示/リンク/戻りURLもこの板に揃える
                        # テンプレ追加なしで通知したいので error_message を流用（赤表示でも致命的ではない）
                        if cur_label and fb_label:
                            error_message = f"0件だったため、板を「{cur_label}」→「{fb_label}」に切り替えて再検索しました。"
                        else:
                            error_message = "0件だったため、板を切り替えて再検索しました。"
                except Exception as e:
                    # フォールバックが失敗しても、元の0件を維持して落とさない
                    error_message = f"外部検索中にエラーが発生しました: {e}"

        if (not error_message) and board_category and board_id:
            board_label = ""
            for b in board_options:
                if b["id"] == board_id:
                    board_label = b["label"]
                    break

            ranking_board_label = board_label or "選択した板"
            ranking_board = get_board_ranking(area, board_category, board_id)

            if ranking_board:
                ranking_source_url = RANKING_URL_TEMPLATE.format(
                    acode=area,
                    ctgid=board_category,
                    bid=board_id,
                )

        # ★履歴：no_log=1 のときは積まない（戻っただけで増えるのを防ぐ）
        # ★KB経由も no_log 扱いで積まない
        if not error_message and not no_log_flag:
            _touch_external_history(db, area, period, board_category, board_id, keyword)

    # この画面の「正しい戻り先URL」（条件一式 + no_log=1）
    back_url = _build_thread_search_url(
        area=area,
        period=period,
        board_category=board_category,
        board_id=board_id,
        keyword=keyword,
    )

    recent_external_searches = _build_recent_external_searches(db, limit=15)

    saved_flag = request.query_params.get("saved")
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
            "ranking_board": ranking_board,
            "ranking_board_label": ranking_board_label,
            "ranking_source_url": ranking_source_url,
            "back_url": back_url,  # ★テンプレはこれだけ使う
        },
    )


@router.api_route("/thread_search/save", methods=["GET", "POST"])
async def save_external_thread(
    request: Request,
    db: Session = Depends(get_db),
):
    # back_url を優先（なければ referer）
    back_url = ""
    try:
        if request.method == "POST":
            form = await request.form()
            back_url = (form.get("back_url") or "").strip()
        else:
            back_url = (request.query_params.get("back_url") or "").strip()
    except Exception:
        back_url = ""

    if not back_url:
        back_url = request.headers.get("referer") or "/thread_search"
        if back_url and "/thread_search/posts" in back_url:
            back_url = "/thread_search"

    back_url = _safe_back_url(back_url, default="/thread_search")

    url = ""
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

    if not is_valid_bakusai_thread_url(url):
        return RedirectResponse(url=back_url, status_code=303)

    try:
        fetch_thread_into_db(db, url)
        redirect_to = _add_flag_to_url(back_url, "saved")
    except Exception:
        db.rollback()
        redirect_to = back_url

    return RedirectResponse(url=redirect_to, status_code=303)


@router.get("/thread_search/showall", response_class=HTMLResponse)
def thread_showall_page(
    request: Request,
    url: str = "",
    area: str = DEFAULT_AREA,
    period: str = DEFAULT_PERIOD,
    title_keyword: str = "",
    view: str = "tree",
    board_category: str = DEFAULT_BOARD_CATEGORY,
    board_id: str = DEFAULT_BOARD_ID_BY_CATEGORY.get(DEFAULT_BOARD_CATEGORY, ""),
    back_url: str = "",
    db: Session = Depends(get_db),
):
    url = (url or "").strip()

    area, period, board_category, board_id, title_keyword = _normalize_thread_search_params(
        area, period, board_category, board_id, title_keyword
    )

    view = (view or "").strip().lower()
    if view not in ("tree", "flat"):
        view = "tree"

    back_url = _safe_back_url(
        back_url,
        default=_build_thread_search_url(area, period, board_category, board_id, title_keyword),
    )

    # ★ここが今回の肝：showall でもラベルを作る（未定義エラー回避）
    board_category_label: str = ""
    board_label: str = ""

    if board_category:
        board_category_label = next(
            (c["label"] for c in BOARD_CATEGORY_OPTIONS if c.get("id") == board_category),
            board_category,
        )

    if board_category and board_id:
        for b in get_board_options_for_category(board_category):
            if b.get("id") == board_id:
                board_label = b.get("label") or ""
                break

    error_message = ""
    thread_title_display = ""
    posts_sorted: List[object] = []

    tree_roots: List[dict] = []
    posts_unknown: List[object] = []

    if not url:
        error_message = "URLが指定されていません。"
    elif not is_valid_bakusai_thread_url(url):
        error_message = "爆サイのスレURLのみ表示できます。"
    else:
        try:
            try:
                t = get_thread_title(url)
                thread_title_display = simplify_thread_title(t or "")
            except Exception:
                thread_title_display = ""

            all_posts = get_thread_posts_cached(db, url)

            def _post_key(p):
                return p.post_no if getattr(p, "post_no", None) is not None else 10**9

            posts_sorted = sorted(list(all_posts), key=_post_key)

            def _extract_anchors(p) -> List[int]:
                a = getattr(p, "anchors", None)
                if not a:
                    return []
                if isinstance(a, str):
                    try:
                        return parse_anchors_csv(a)
                    except Exception:
                        return []
                if isinstance(a, (list, tuple, set)):
                    out: List[int] = []
                    for x in a:
                        try:
                            n = int(x)
                            if n > 0:
                                out.append(n)
                        except Exception:
                            continue
                    return out
                return []

            nodes_by_no: Dict[int, dict] = {}
            for p in posts_sorted:
                pn = getattr(p, "post_no", None)
                if pn is None:
                    posts_unknown.append(p)
                    continue
                if pn not in nodes_by_no:
                    nodes_by_no[pn] = {"post": p, "children": []}

            for pn, node in nodes_by_no.items():
                p = node["post"]
                anchors = _extract_anchors(p)
                parent_no: Optional[int] = None
                for a in anchors:
                    if a in nodes_by_no and a != pn and a < pn:
                        parent_no = a
                        break

                if parent_no is None:
                    tree_roots.append(node)
                else:
                    nodes_by_no[parent_no]["children"].append(node)

            def _sort_subtree(n: dict) -> None:
                n["children"].sort(key=lambda c: getattr(c["post"], "post_no", None) or 10**9)
                for ch in n["children"]:
                    _sort_subtree(ch)

            tree_roots.sort(key=lambda n: getattr(n["post"], "post_no", None) or 10**9)
            for r in tree_roots:
                _sort_subtree(r)

        except Exception as e:
            error_message = f"全レス取得中にエラーが発生しました: {e}"
            posts_sorted = []
            tree_roots = []
            posts_unknown = []

    store_title = build_store_search_title(thread_title_display or title_keyword)
    store_cityheaven_url = build_google_site_search_url("cityheaven.net", store_title)
    store_dto_url = build_google_site_search_url("dto.jp", store_title)

    return templates.TemplateResponse(
        "thread_showall.html",
        {
            "request": request,
            "thread_url": url,
            "thread_title": thread_title_display,
            "area": area,
            "period": period,
            "title_keyword": title_keyword,
            "view": view,
            "posts": posts_sorted,
            "tree_roots": tree_roots,
            "posts_unknown": posts_unknown,
            "error_message": error_message,
            "store_title": store_title,
            "store_cityheaven_url": store_cityheaven_url,
            "store_dto_url": store_dto_url,
            "board_category": board_category,
            "board_id": board_id,
            "board_category_label": board_category_label,
            "board_label": board_label,
            "back_url": back_url,
        },
    )


@router.post("/thread_search/posts", response_class=HTMLResponse)
def thread_search_posts(
    request: Request,
    selected_thread: str = Form(""),
    title_keyword: str = Form(""),
    post_keyword: str = Form(""),
    area: str = Form(DEFAULT_AREA),
    period: str = Form(DEFAULT_PERIOD),
    board_category: str = Form(DEFAULT_BOARD_CATEGORY),
    board_id: str = Form(DEFAULT_BOARD_ID_BY_CATEGORY.get(DEFAULT_BOARD_CATEGORY, "")),
    back_url: str = Form(""),
    db: Session = Depends(get_db),
):
    # ---- フェーズ1ログ用 PERF: スレ内検索の所要時間内訳ログ（必要なくなったら False に）
    PERF_LOG = True
    t0_all = time.perf_counter()
    def _p(label: str, t_start: float) -> float:
        if PERF_LOG:
            dt_ms = (time.perf_counter() - t_start) * 1000.0
            logging.info("[PERF][thread_search_posts] %s: %.1f ms", label, dt_ms)
        return time.perf_counter()

    selected_thread = (selected_thread or "").strip()
    post_keyword = (post_keyword or "").strip()

    area, period, board_category, board_id, title_keyword = _normalize_thread_search_params(
        area, period, board_category, board_id, title_keyword
    )

    back_url = _safe_back_url(
        back_url,
        default=_build_thread_search_url(area, period, board_category, board_id, title_keyword),
    )

    entries: List[dict] = []
    error_message = ""
    thread_title_display: str = ""
    prev_thread_url: Optional[str] = None
    next_thread_url: Optional[str] = None

    # ★④：prev/next のスレタイ
    prev_thread_title: str = ""
    next_thread_title: str = ""

    board_category_label: str = ""
    board_label: str = ""

    if not selected_thread:
        error_message = "スレッドが選択されていません。"
    elif not post_keyword:
        error_message = "本文キーワードが入力されていません。"
    elif not is_valid_bakusai_thread_url(selected_thread):
        error_message = "爆サイのスレURLのみ検索できます。"
    else:
        try:
            # ★003機能移植：タイトルはDBキャッシュ経由（失敗しても空でOK）
            thread_title_display = _get_thread_title_cached(db, selected_thread)

            if board_category:
                for c in BOARD_CATEGORY_OPTIONS:
                    if c["id"] == board_category:
                        board_category_label = c["label"]
                        break

            if board_category and board_id:
                for b in get_board_options_for_category(board_category):
                    if b["id"] == board_id:
                        board_label = b["label"]
                        break

            prev_thread_url, next_thread_url = find_prev_next_thread_urls(selected_thread)

            # prev/next のタイトルもキャッシュ経由（失敗しても空でOK）
            if prev_thread_url:
                prev_thread_title = _get_thread_title_cached(db, prev_thread_url)
            if next_thread_url:
                next_thread_title = _get_thread_title_cached(db, next_thread_url)

            # フェーズ1ログ用
            tA = time.perf_counter()
            # これは消したらダメ
            all_posts = get_thread_posts_cached(db, selected_thread)
            # フェーズ1ログ用
            tA = _p("A) get_thread_posts_cached()", tA)


            def _post_key(p):
                return p.post_no if getattr(p, "post_no", None) is not None else 10**9

            # フェーズ1ログ用
            tB = time.perf_counter()
            # これは消したらダメ
            all_posts_sorted = sorted(list(all_posts), key=_post_key)
            # フェーズ1ログ用
            tB = _p("B) sort(all_posts)", tB)

            # 追加ここから：post_no索引 & 本文正規化の使い回し（リクエスト内キャッシュ）
            posts_by_no: Dict[int, object] = {}
            body_norm_by_no: Dict[int, str] = {}
            
            for p in all_posts_sorted:
                pn = getattr(p, "post_no", None)
                if pn is None:
                    continue
            
                # post_no -> post（最初に出たものを採用）
                if pn not in posts_by_no:
                    posts_by_no[pn] = p
            
                # post_no -> normalize(body)
                # （検索語を変えても同一リクエスト内では再計算しない）
                if pn not in body_norm_by_no:
                    body_norm_by_no[pn] = normalize_for_search(getattr(p, "body", "") or "")
            # 追加ここまで
            # フェーズ1ログ用
            tB = _p("B2) build indexes (posts_by_no/body_norm_by_no)", tB)

            # フェーズ1ログ用
            tC = time.perf_counter()
            
            replies: Dict[int, List[object]] = defaultdict(list)
            for p in all_posts_sorted:
                if not getattr(p, "anchors", None):
                    continue
                for a in p.anchors:
                    replies[a].append(p)

            # フェーズ1ログ用
            tC = _p("C) build replies map (anchors->posts)", tC)


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

            # フェーズ1ログ用
            tD = time.perf_counter()
            post_keyword_norm = normalize_for_search(post_keyword)

            for root in all_posts_sorted:
                pn = getattr(root, "post_no", None)
                if pn is None:
                    continue
            
                body_norm = body_norm_by_no.get(pn, "")
                if not body_norm or (post_keyword_norm not in body_norm):
                    continue

                context_posts: List[object] = []
                pn = getattr(root, "post_no", None)
                if pn is not None:
                    start_no = max(1, pn - 5)
                    end_no = pn + 5
                    for n in range(start_no, end_no + 1):
                        hit_p = posts_by_no.get(n)
                        if hit_p is not None:
                            context_posts.append(hit_p)

                tree_items = build_reply_tree_external(root)

                anchor_targets: List[object] = []
                if getattr(root, "anchors", None):
                    for n in root.anchors:
                        target = posts_by_no.get(n)
                        if target:
                            anchor_targets.append(target)

                entries.append(
                    {
                        "root": root,
                        "context": context_posts,
                        "tree": tree_items,
                        "anchor_targets": anchor_targets,
                    }
                )

            # フェーズ1ログ用
            tD = _p("D) scan hits + build entries (context/tree/anchors)", tD)
            if PERF_LOG:
                logging.info(
                    "[PERF][thread_search_posts] posts=%d hits=%d",
                    len(all_posts_sorted),
                    len(entries),
                )



        except Exception as e:
            error_message = f"スレッド内検索中にエラーが発生しました: {e}"
            entries = []

    store_base_title = build_store_search_title(thread_title_display or title_keyword)
    store_cityheaven_url = build_google_site_search_url("cityheaven.net", store_base_title)
    store_dto_url = build_google_site_search_url("dto.jp", store_base_title)

    # フェーズ1ログ用
    _p("Z) total thread_search_posts()", t0_all)
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
            "prev_thread_title": prev_thread_title,
            "next_thread_title": next_thread_title,
            "highlight_with_links": highlight_with_links,
            "board_category": board_category,
            "board_id": board_id,
            "board_category_label": board_category_label,
            "board_label": board_label,
            "store_base_title": store_base_title,
            "store_cityheaven_url": store_cityheaven_url,
            "store_dto_url": store_dto_url,
            "back_url": back_url,
        },
    )
