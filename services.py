# 001
# services.py
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from urllib.parse import quote_plus, urlparse
from types import SimpleNamespace

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy import func, text, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from constants import MAX_CACHED_THREADS
from models import ThreadPost, CachedThread, CachedPost, ThreadMeta
from scraper import fetch_posts_from_thread, get_thread_title, ScrapingError
from utils import simplify_thread_title, normalize_for_search, parse_anchors_csv, parse_posted_at_value

try:
    from zoneinfo import ZoneInfo
    JST = ZoneInfo("Asia/Tokyo")
except Exception:
    JST = None


# 保存済みレスは維持したまま、外部サイトの確認だけ短い間隔で行う。
THREAD_INCREMENTAL_CHECK_INTERVAL = timedelta(minutes=5)
THREAD_FULL_REPAIR_INTERVAL = timedelta(hours=24)
THREAD_MISSING_ANCHOR_REPAIR_INTERVAL = timedelta(hours=6)


# =========================
# SSRF 対策：URL制限
# =========================
def is_valid_bakusai_thread_url(u: str) -> bool:
    """SSRF対策：取得対象URLを爆サイのスレURLに限定する。"""
    if not u:
        return False
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
    if "/thr_res/" not in path and "/thr_res_show/" not in path:
        return False

    return True


def _require_valid_bakusai_url(u: str) -> str:
    u = (u or "").strip()
    if not u or not is_valid_bakusai_thread_url(u):
        raise ValueError("爆サイのスレURLのみ処理できます。")
    return u


# =========================
# thread_url の canonical 化（キー統一用）
# =========================
def _canonicalize_thread_url_key(raw: str) -> str:
    """
    同一スレッドが常に同じキーになるように正規化（DBキー用）。
    - query / fragment を落とす
    - rrid=xx が末尾に混ざっても落とす
    - http -> https
    - thr_res_show -> thr_res に寄せる（canonical）
    - 末尾スラッシュ統一
    """
    u = (raw or "").strip()
    if not u:
        return ""

    u = u.split("#", 1)[0]
    u = u.split("?", 1)[0]
    u = re.sub(r"rrid=\d+/?$", "", u)

    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]

    u = u.replace("/thr_res_show/", "/thr_res/")

    if u and not u.endswith("/"):
        u += "/"

    return u


def _alt_show_thread_url(canonical_thr_res_url: str) -> str:
    """canonical(thr_res) から show(thr_res_show) 版のキーも作る。"""
    if not canonical_thr_res_url:
        return ""
    return canonical_thr_res_url.replace("/thr_res/", "/thr_res_show/")


# =========================
# ThreadPost / ThreadMeta のキー移行（show->res 等）
# =========================
def _migrate_thread_posts_key_if_needed(db: Session, old_url: str, new_url: str) -> None:
    if not old_url or not new_url or old_url == new_url:
        return
    try:
        exists_old = db.query(ThreadPost.id).filter(ThreadPost.thread_url == old_url).first()
        if not exists_old:
            return

        db.query(ThreadPost).filter(ThreadPost.thread_url == old_url).update(
            {ThreadPost.thread_url: new_url},
            synchronize_session=False,
        )
        db.commit()
    except Exception:
        db.rollback()


def _migrate_thread_meta_key_if_needed(db: Session, old_url: str, new_url: str) -> None:
    if not old_url or not new_url or old_url == new_url:
        return
    try:
        old = db.query(ThreadMeta).filter(ThreadMeta.thread_url == old_url).first()
        if not old:
            return
        new = db.query(ThreadMeta).filter(ThreadMeta.thread_url == new_url).first()

        if not new:
            old.thread_url = new_url
            db.commit()
            return

        if (not (new.label or "").strip()) and (old.label or "").strip():
            new.label = old.label
        db.delete(old)
        db.commit()
    except Exception:
        db.rollback()


def _migrate_cache_key_if_needed(db: Session, old_url: str, new_url: str) -> None:
    if not old_url or not new_url or old_url == new_url:
        return

    try:
        exists_new = db.query(CachedThread).filter(CachedThread.thread_url == new_url).first()
        if exists_new:
            return

        meta_old = db.query(CachedThread).filter(CachedThread.thread_url == old_url).first()
        if not meta_old:
            return

        db.query(CachedPost).filter(CachedPost.thread_url == old_url).update(
            {CachedPost.thread_url: new_url},
            synchronize_session=False,
        )
        db.query(CachedThread).filter(CachedThread.thread_url == old_url).update(
            {CachedThread.thread_url: new_url},
            synchronize_session=False,
        )
        db.commit()
    except Exception:
        db.rollback()


# =========================
# 重複掃除 / posted_at_dt バックフィル
# =========================
def cleanup_thread_posts_duplicates(db: Session) -> None:
    """(thread_url, post_no) が重複しているレコードを掃除する。"""
    try:
        db.execute(
            text(
                """
                DELETE FROM thread_posts a
                USING thread_posts b
                WHERE a.id > b.id
                  AND a.thread_url = b.thread_url
                  AND a.post_no = b.post_no
                  AND a.post_no IS NOT NULL
                """
            )
        )
        db.commit()
    except Exception:
        db.rollback()


def backfill_posted_at_dt(db: Session, limit: int = 5000) -> None:
    """posted_at(Text) -> posted_at_dt(DateTime) を未設定分だけ埋める。"""
    try:
        rows = (
            db.query(ThreadPost)
            .filter(ThreadPost.posted_at.isnot(None))
            .filter(ThreadPost.posted_at_dt.is_(None))
            .order_by(ThreadPost.id.asc())
            .limit(limit)
            .all()
        )
        changed = 0
        for p in rows:
            dt = parse_posted_at_value(p.posted_at or "")
            if dt:
                p.posted_at_dt = dt
                changed += 1
        if changed:
            db.commit()
    except Exception:
        db.rollback()


def backfill_norm_columns(db: Session, max_total: int = 300000, batch_size: int = 5000) -> int:
    """揺らぎ検索用の正規化列（*_norm）をバックフィルする。"""
    processed = 0
    if max_total <= 0 or batch_size <= 0:
        return 0

    try:
        while processed < max_total:
            rows = (
                db.query(ThreadPost)
                .filter(
                    or_(
                        ThreadPost.body_norm.is_(None),
                        ThreadPost.thread_title_norm.is_(None),
                        ThreadPost.tags_norm.is_(None),
                    )
                )
                .order_by(ThreadPost.id.asc())
                .limit(min(batch_size, max_total - processed))
                .all()
            )

            if not rows:
                break

            for p in rows:
                if p.body_norm is None:
                    p.body_norm = normalize_for_search(p.body or "")
                if p.thread_title_norm is None:
                    p.thread_title_norm = normalize_for_search(p.thread_title or "")
                if p.tags_norm is None:
                    p.tags_norm = normalize_for_search(p.tags or "")

            db.commit()
            processed += len(rows)
    except Exception:
        db.rollback()

    return processed


# =========================
# スレ取り込み（内部DB: thread_posts）
# =========================
def fetch_thread_into_db(db: Session, url: str) -> int:
    """爆サイスレURLをスクレイピングして thread_posts に追記する。"""
    raw_url = _require_valid_bakusai_url(url)
    canonical_url = _canonicalize_thread_url_key(raw_url)
    if not canonical_url:
        raise ValueError("URLの正規化に失敗しました。")

    alt_show_url = _alt_show_thread_url(canonical_url)

    _migrate_thread_posts_key_if_needed(db, raw_url, canonical_url)
    if alt_show_url and alt_show_url != canonical_url:
        _migrate_thread_posts_key_if_needed(db, alt_show_url, canonical_url)

    _migrate_thread_meta_key_if_needed(db, raw_url, canonical_url)
    if alt_show_url and alt_show_url != canonical_url:
        _migrate_thread_meta_key_if_needed(db, alt_show_url, canonical_url)

    if alt_show_url and alt_show_url != canonical_url:
        _migrate_cache_key_if_needed(db, alt_show_url, canonical_url)

    last_no = (
        db.query(func.max(ThreadPost.post_no))
        .filter(ThreadPost.thread_url == canonical_url)
        .scalar()
    )
    if last_no is None:
        last_no = 0

    thread_title = ""
    try:
        title = get_thread_title(raw_url)
        if title:
            thread_title = simplify_thread_title(title)
    except Exception:
        thread_title = ""

    if thread_title:
        db.query(ThreadPost).filter(
            ThreadPost.thread_url == canonical_url,
            ThreadPost.thread_title.is_(None),
        ).update(
            {ThreadPost.thread_title: thread_title},
            synchronize_session=False,
        )

    scraped_posts = fetch_posts_from_thread(canonical_url)
    count = 0

    for sp in scraped_posts:
        body = (getattr(sp, "body", None) or "").strip()
        if not body:
            continue

        sp_no = getattr(sp, "post_no", None)
        if sp_no is not None and sp_no <= last_no:
            continue

        anchors_list = getattr(sp, "anchors", None)
        anchors_str = "," + ",".join(str(a) for a in anchors_list) + "," if anchors_list else None

        posted_at_raw = getattr(sp, "posted_at", None)
        posted_at_dt = parse_posted_at_value(posted_at_raw or "") if posted_at_raw else None

        if sp_no is not None:
            existing = (
                db.query(ThreadPost)
                .filter(ThreadPost.thread_url == canonical_url, ThreadPost.post_no == sp_no)
                .first()
            )
        else:
            existing = (
                db.query(ThreadPost)
                .filter(ThreadPost.thread_url == canonical_url, ThreadPost.body == body)
                .first()
            )

        if existing:
            if not existing.posted_at and posted_at_raw:
                existing.posted_at = posted_at_raw
            if existing.posted_at_dt is None and posted_at_dt is not None:
                existing.posted_at_dt = posted_at_dt
            if not existing.anchors and anchors_str:
                existing.anchors = anchors_str
            if thread_title and not existing.thread_title:
                existing.thread_title = thread_title
            continue

        db.add(
            ThreadPost(
                thread_url=canonical_url,
                thread_title=thread_title or None,
                post_no=sp_no,
                posted_at=posted_at_raw,
                posted_at_dt=posted_at_dt,
                body=body,
                anchors=anchors_str,
            )
        )
        count += 1

    db.commit()
    return count


# =========================
# 外部検索：爆サイのスレッド検索（期間フィルタは JST 基準）
# =========================
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

    url = base + "p=1/sch=thr_sch/sch_range=board/word=" + quote_plus(keyword) + "/"

    resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    threads: List[dict] = []

    threshold: Optional[datetime] = None
    if max_days is not None:
        if JST is not None:
            now_jst = datetime.now(JST).replace(tzinfo=None)
        else:
            now_jst = datetime.now()
        threshold = now_jst - timedelta(days=max_days)

    keyword_norm = normalize_for_search(keyword)

    for s in soup.find_all(string=re.compile("最新レス投稿日時")):
        text_s = str(s)
        match = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", text_s)
        if not match:
            continue
        try:
            dt = datetime.strptime(match.group(1), "%Y/%m/%d %H:%M")
        except ValueError:
            continue

        if threshold is not None and dt < threshold:
            continue

        parent = s.parent
        link = None
        while parent is not None and getattr(parent, "name", None) not in ("html", "body"):
            candidate = parent.find("a", href=True)
            if candidate and "/thr_res/" in (candidate.get("href", "") or ""):
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

        href = (link.get("href", "") or "").strip()
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

    unique_by_url: Dict[str, dict] = {}
    for thread in threads:
        if thread["url"] not in unique_by_url:
            unique_by_url[thread["url"]] = thread

    result = list(unique_by_url.values())
    result.sort(key=lambda x: x.get("last_post_at_str") or "", reverse=True)
    return result


# =========================
# 前後スレ探索（爆サイページャー）
# =========================
def _normalize_bakusai_href(href: str) -> str:
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://bakusai.com" + href
    return href


def find_prev_next_thread_urls(thread_url: str) -> Tuple[Optional[str], Optional[str]]:
    """スレページから prev/next を拾う。"""
    try:
        thread_url = _require_valid_bakusai_url(thread_url)
    except Exception:
        return (None, None)

    try:
        resp = requests.get(thread_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
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
        href = (a.get("href", "") or "").strip()
        if not href:
            return None
        normalized = _normalize_bakusai_href(href)
        return normalized if is_valid_bakusai_thread_url(normalized) else None

    return (pick_url(prev_div), pick_url(next_div))


# =========================
# 外部検索：スレ全文キャッシュ（DB）
# =========================
def _evict_old_cached_threads(db: Session) -> None:
    try:
        count = db.query(func.count(CachedThread.thread_url)).scalar() or 0
        if count <= MAX_CACHED_THREADS:
            return

        over = count - MAX_CACHED_THREADS
        old_threads = (
            db.query(CachedThread)
            .order_by(CachedThread.last_accessed_at.asc())
            .limit(over)
            .all()
        )

        for thread in old_threads:
            db.query(CachedPost).filter(CachedPost.thread_url == thread.thread_url).delete(
                synchronize_session=False
            )
            db.query(CachedThread).filter(CachedThread.thread_url == thread.thread_url).delete(
                synchronize_session=False
            )

        db.commit()
    except Exception:
        db.rollback()


def _save_thread_posts_to_cache(
    db: Session,
    thread_url: str,
    posts: List[object],
    *,
    full_refresh: bool,
) -> None:
    """
    レス番号ありはUPSERT、レス番号なしは本文と日時で重複を避けて保存する。
    fetched_at は最終全件補修、last_accessed_at は最終外部確認として扱う。
    """
    now = datetime.utcnow()
    numbered_rows: Dict[int, dict] = {}
    unknown_rows: List[dict] = []
    seen_unknown: set[tuple[Optional[str], str]] = set()

    for post in posts:
        body = (getattr(post, "body", None) or "").strip()
        if not body:
            continue

        post_no = getattr(post, "post_no", None)
        posted_at = getattr(post, "posted_at", None)
        anchors_list = getattr(post, "anchors", None)
        anchors_str = "," + ",".join(str(a) for a in anchors_list) + "," if anchors_list else None

        row = {
            "thread_url": thread_url,
            "post_no": post_no,
            "posted_at": posted_at,
            "body": body,
            "anchors": anchors_str,
        }

        if post_no is not None:
            numbered_rows[int(post_no)] = row
        else:
            key = (posted_at, body)
            if key in seen_unknown:
                continue
            seen_unknown.add(key)
            unknown_rows.append(row)

    meta = db.query(CachedThread).filter(CachedThread.thread_url == thread_url).first()
    if not meta:
        meta = CachedThread(thread_url=thread_url, fetched_at=now, last_accessed_at=now)
        db.add(meta)
    else:
        if full_refresh:
            meta.fetched_at = now
        meta.last_accessed_at = now

    rows = list(numbered_rows.values())
    if rows:
        stmt = pg_insert(CachedPost).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[CachedPost.thread_url, CachedPost.post_no],
            set_={
                "posted_at": stmt.excluded.posted_at,
                "body": stmt.excluded.body,
                "anchors": stmt.excluded.anchors,
            },
        )
        db.execute(stmt)

    for row in unknown_rows:
        exists = (
            db.query(CachedPost.id)
            .filter(
                CachedPost.thread_url == thread_url,
                CachedPost.post_no.is_(None),
                CachedPost.posted_at == row["posted_at"],
                CachedPost.body == row["body"],
            )
            .first()
        )
        if not exists:
            db.add(CachedPost(**row))

    db.commit()
    _evict_old_cached_threads(db)


def _load_thread_posts_from_cache(db: Session, thread_url: str) -> List[CachedPost]:
    return (
        db.query(CachedPost)
        .filter(CachedPost.thread_url == thread_url)
        .order_by(CachedPost.post_no.asc().nullslast(), CachedPost.id.asc())
        .all()
    )


def _cache_has_missing_anchor_targets(rows: List[CachedPost]) -> bool:
    present = {int(row.post_no) for row in rows if row.post_no is not None}
    if not present:
        return False

    max_present = max(present)
    for row in rows:
        for anchor in parse_anchors_csv(row.anchors):
            if 0 < anchor <= max_present and anchor not in present:
                return True
    return False


def _max_cached_post_no(db: Session, thread_url: str) -> Optional[int]:
    value = (
        db.query(func.max(CachedPost.post_no))
        .filter(CachedPost.thread_url == thread_url)
        .scalar()
    )
    return int(value) if value is not None else None


def _refresh_cached_thread(
    db: Session,
    thread_url: str,
    *,
    full_refresh: bool,
) -> None:
    stop_at_post_no = None if full_refresh else _max_cached_post_no(db, thread_url)
    posts = fetch_posts_from_thread(
        thread_url,
        stop_at_post_no=stop_at_post_no,
    )
    _save_thread_posts_to_cache(
        db,
        thread_url,
        list(posts),
        full_refresh=full_refresh or stop_at_post_no is None,
    )


def get_thread_posts_cached(db: Session, thread_url: str) -> List[object]:
    """
    保存済みレスはDBに維持し、通常は最新側だけを増分取得する。
    - 外部確認は最短5分間隔
    - 24時間ごとに全ページを補修
    - キャッシュ内でアンカー先欠落を検出した場合は、最終全件取得から6時間以上なら補修
    - 外部取得失敗時も既存キャッシュがあれば検索を継続する
    """
    try:
        raw_url = _require_valid_bakusai_url(thread_url)
    except Exception:
        return []

    canonical_url = _canonicalize_thread_url_key(raw_url)
    if not canonical_url:
        return []

    alt_show_url = _alt_show_thread_url(canonical_url)
    _migrate_cache_key_if_needed(db, alt_show_url, canonical_url)

    meta = db.query(CachedThread).filter(CachedThread.thread_url == canonical_url).first()
    if meta is None:
        meta = db.query(CachedThread).filter(CachedThread.thread_url == alt_show_url).first()
        if meta is not None:
            _migrate_cache_key_if_needed(db, alt_show_url, canonical_url)
            meta = db.query(CachedThread).filter(CachedThread.thread_url == canonical_url).first()

    now = datetime.utcnow()
    did_full_refresh = False

    full_refresh_due = (
        meta is None
        or meta.fetched_at is None
        or now - meta.fetched_at >= THREAD_FULL_REPAIR_INTERVAL
    )
    incremental_check_due = (
        meta is None
        or meta.last_accessed_at is None
        or now - meta.last_accessed_at >= THREAD_INCREMENTAL_CHECK_INTERVAL
    )

    try:
        if full_refresh_due:
            _refresh_cached_thread(db, canonical_url, full_refresh=True)
            did_full_refresh = True
        elif incremental_check_due:
            _refresh_cached_thread(db, canonical_url, full_refresh=False)
    except Exception as exc:
        db.rollback()
        logging.warning(
            "[THREAD_CACHE][refresh_failed] url=%s full=%s error=%s",
            canonical_url,
            did_full_refresh or full_refresh_due,
            exc,
        )

    cached_rows = _load_thread_posts_from_cache(db, canonical_url)

    meta = db.query(CachedThread).filter(CachedThread.thread_url == canonical_url).first()
    missing_anchor_repair_due = (
        not did_full_refresh
        and bool(cached_rows)
        and _cache_has_missing_anchor_targets(cached_rows)
        and (
            meta is None
            or meta.fetched_at is None
            or now - meta.fetched_at >= THREAD_MISSING_ANCHOR_REPAIR_INTERVAL
        )
    )

    if missing_anchor_repair_due:
        try:
            _refresh_cached_thread(db, canonical_url, full_refresh=True)
            cached_rows = _load_thread_posts_from_cache(db, canonical_url)
        except Exception as exc:
            db.rollback()
            logging.warning(
                "[THREAD_CACHE][repair_failed] url=%s error=%s",
                canonical_url,
                exc,
            )

    result: List[object] = []
    for row in cached_rows:
        result.append(
            SimpleNamespace(
                post_no=row.post_no,
                posted_at=row.posted_at,
                body=row.body,
                anchors=parse_anchors_csv(row.anchors),
            )
        )
    return result
