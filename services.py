# services.py
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from urllib.parse import quote_plus, urlparse
from types import SimpleNamespace

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from constants import THREAD_CACHE_TTL, MAX_CACHED_THREADS
from models import ThreadPost, CachedThread, CachedPost
from scraper import fetch_posts_from_thread, get_thread_title, ScrapingError
from utils import (
    simplify_thread_title,
    normalize_for_search,
    parse_anchors_csv,
    parse_posted_at_value,
)

try:
    from zoneinfo import ZoneInfo
    JST = ZoneInfo("Asia/Tokyo")
except Exception:
    JST = None


# =========================
# SSRF 対策：URL制限
# =========================
def is_valid_bakusai_thread_url(u: str) -> bool:
    """
    SSRF対策：取得対象URLを爆サイのスレURLに限定する
    """
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
# ★追加：thread_url の canonical 化（キャッシュキー統一用）
# =========================
def _canonicalize_thread_url_key(raw: str) -> str:
    """
    同一スレッドが常に同じキーになるように正規化（キャッシュ/保存用）。
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
    if not canonical_thr_res_url:
        return ""
    return canonical_thr_res_url.replace("/thr_res/", "/thr_res_show/")


def _migrate_cache_key_if_needed(db: Session, old_url: str, new_url: str) -> None:
    """
    既存の CachedThread/CachedPost が old_url で保存されている場合、
    それを new_url（canonical）に移し替える。
    """
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
        return


def _migrate_thread_posts_key_if_needed(db: Session, old_url: str, new_url: str) -> None:
    """
    thread_posts 側も URL 揺れをできるだけ統一する（show/res 混在で検索・プレビューがズレるのを防ぐ）。
    - new_url 側が既に存在するなら衝突回避で何もしない
    """
    if not old_url or not new_url or old_url == new_url:
        return
    try:
        exists_new = db.query(ThreadPost.id).filter(ThreadPost.thread_url == new_url).first()
        if exists_new:
            return
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


def _norm_tags(tags: Optional[str]) -> str:
    if not tags:
        return ""
    # カンマ区切りを空白に寄せて、normalize_for_search へ
    return normalize_for_search(tags.replace(",", " ")).strip()


# =========================
# 重複掃除 / posted_at_dt バックフィル
# =========================
def cleanup_thread_posts_duplicates(db: Session) -> None:
    """
    (thread_url, post_no) が重複しているレコードを掃除（post_no が NULL のものは対象外）。
    最小 id を残し、それ以外を削除。
    """
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
    """
    posted_at(Text) -> posted_at_dt(DateTime) を埋める（未設定分だけ）
    """
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


def backfill_norm_columns(db: Session, max_total: int = 300000, batch_size: int = 5000) -> None:
    """
    ★追加：内部検索の揺らぎ対応用に body_norm / thread_title_norm / tags_norm を埋める。
    可能な範囲で起動時にまとめて埋めて「どっきり→ドッキリ」等を復活させる。
    """
    done = 0
    while done < max_total:
        try:
            rows = (
                db.query(ThreadPost)
                .filter(
                    (ThreadPost.body_norm.is_(None)) |
                    (ThreadPost.thread_title_norm.is_(None)) |
                    (ThreadPost.tags_norm.is_(None))
                )
                .order_by(ThreadPost.id.asc())
                .limit(batch_size)
                .all()
            )
            if not rows:
                break

            changed = 0
            for p in rows:
                if p.body_norm is None:
                    p.body_norm = normalize_for_search(p.body or "")
                    changed += 1
                if p.thread_title_norm is None:
                    p.thread_title_norm = normalize_for_search(p.thread_title or "")
                    changed += 1
                if p.tags_norm is None:
                    p.tags_norm = _norm_tags(p.tags)
                    changed += 1

            if changed:
                db.commit()
            else:
                break

            done += len(rows)
        except Exception:
            db.rollback()
            break


# =========================
# スレ取り込み（内部DB: thread_posts）
# =========================
def fetch_thread_into_db(db: Session, url: str) -> int:
    """
    爆サイスレURLをスクレイピングして thread_posts に追記する（既存は重複回避）
    """
    url = _require_valid_bakusai_url(url)

    # ★保存側も canonical に寄せる（show/res 揺れを減らす）
    canonical_url = _canonicalize_thread_url_key(url)
    if canonical_url:
        alt_show = _alt_show_thread_url(canonical_url)
        _migrate_thread_posts_key_if_needed(db, alt_show, canonical_url)
        url = canonical_url

    last_no = (
        db.query(func.max(ThreadPost.post_no))
        .filter(ThreadPost.thread_url == url)
        .scalar()
    )
    if last_no is None:
        last_no = 0

    # タイトル取得・簡略化
    thread_title = ""
    try:
        t = get_thread_title(url)
        if t:
            thread_title = simplify_thread_title(t)
    except Exception:
        thread_title = ""

    thread_title_norm = normalize_for_search(thread_title) if thread_title else ""

    # 既存で thread_title が空のものに入れておく（norm も一緒に）
    if thread_title:
        db.query(ThreadPost).filter(
            ThreadPost.thread_url == url,
            ThreadPost.thread_title.is_(None),
        ).update(
            {
                ThreadPost.thread_title: thread_title,
                ThreadPost.thread_title_norm: thread_title_norm or None,
            },
            synchronize_session=False,
        )

    scraped_posts = fetch_posts_from_thread(url)
    count = 0

    for sp in scraped_posts:
        body = (getattr(sp, "body", None) or "").strip()
        if not body:
            continue

        sp_no = getattr(sp, "post_no", None)
        if sp_no is not None and sp_no <= last_no:
            continue

        anchors_list = getattr(sp, "anchors", None)
        if anchors_list:
            anchors_str = "," + ",".join(str(a) for a in anchors_list) + ","
        else:
            anchors_str = None

        posted_at_raw = getattr(sp, "posted_at", None)
        posted_at_dt = parse_posted_at_value(posted_at_raw or "") if posted_at_raw else None

        body_norm = normalize_for_search(body)

        # すでに同じレスが入っていれば更新のみ
        if sp_no is not None:
            existing = (
                db.query(ThreadPost)
                .filter(ThreadPost.thread_url == url, ThreadPost.post_no == sp_no)
                .first()
            )
        else:
            existing = (
                db.query(ThreadPost)
                .filter(ThreadPost.thread_url == url, ThreadPost.body == body)
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
            if existing.thread_title_norm is None and thread_title_norm:
                existing.thread_title_norm = thread_title_norm
            if existing.body_norm is None and body_norm:
                existing.body_norm = body_norm
            continue

        db.add(
            ThreadPost(
                thread_url=url,
                thread_title=thread_title or None,
                thread_title_norm=thread_title_norm or None,
                post_no=sp_no,
                posted_at=posted_at_raw,
                posted_at_dt=posted_at_dt,
                body=body,
                body_norm=body_norm or None,
                anchors=anchors_str,
                tags_norm=None,
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

    url = (
        base
        + "p=1/sch=thr_sch/sch_range=board/word="
        + quote_plus(keyword)
        + "/"
    )

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
        m = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2})", text_s)
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
    for t in threads:
        if t["url"] not in unique_by_url:
            unique_by_url[t["url"]] = t

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
    """
    スレページから prev/next を拾う
    """
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
        u = _normalize_bakusai_href(href)
        return u if is_valid_bakusai_thread_url(u) else None

    return (pick_url(prev_div), pick_url(next_div))


# =========================
# 外部検索：スレ全文キャッシュ（DB）
# =========================
def _evict_old_cached_threads(db: Session) -> None:
    try:
        cnt = db.query(func.count(CachedThread.thread_url)).scalar() or 0
        if cnt <= MAX_CACHED_THREADS:
            return

        over = cnt - MAX_CACHED_THREADS
        old_threads = (
            db.query(CachedThread)
            .order_by(CachedThread.last_accessed_at.asc())
            .limit(over)
            .all()
        )

        for t in old_threads:
            db.query(CachedPost).filter(CachedPost.thread_url == t.thread_url).delete(synchronize_session=False)
            db.query(CachedThread).filter(CachedThread.thread_url == t.thread_url).delete(synchronize_session=False)

        db.commit()
    except Exception:
        db.rollback()


def _save_thread_posts_to_cache(db: Session, thread_url: str, posts: List[object]) -> None:
    now = datetime.utcnow()

    db.query(CachedPost).filter(CachedPost.thread_url == thread_url).delete(synchronize_session=False)

    bulk = []
    for p in posts:
        body = (getattr(p, "body", None) or "").strip()
        if not body:
            continue

        post_no = getattr(p, "post_no", None)
        posted_at = getattr(p, "posted_at", None)

        anchors_list = getattr(p, "anchors", None)
        if anchors_list:
            anchors_str = "," + ",".join(str(a) for a in anchors_list) + ","
        else:
            anchors_str = None

        bulk.append(
            CachedPost(
                thread_url=thread_url,
                post_no=post_no,
                posted_at=posted_at,
                body=body,
                anchors=anchors_str,
            )
        )

    if bulk:
        db.bulk_save_objects(bulk)

    meta = db.query(CachedThread).filter(CachedThread.thread_url == thread_url).first()
    if not meta:
        meta = CachedThread(thread_url=thread_url, fetched_at=now, last_accessed_at=now)
        db.add(meta)
    else:
        meta.fetched_at = now
        meta.last_accessed_at = now

    db.commit()
    _evict_old_cached_threads(db)


def _load_thread_posts_from_cache(db: Session, thread_url: str) -> List[CachedPost]:
    return (
        db.query(CachedPost)
        .filter(CachedPost.thread_url == thread_url)
        .order_by(CachedPost.post_no.asc().nullslast(), CachedPost.id.asc())
        .all()
    )


def get_thread_posts_cached(db: Session, thread_url: str) -> List[object]:
    """
    - キャッシュがTTL内ならDBから返す
    - 期限切れ/未作成ならWebから取得してDB保存して返す
    返り値は「scraperのpostっぽい形（SimpleNamespace）」にして templates を壊さない
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

    now = datetime.utcnow()
    meta = db.query(CachedThread).filter(CachedThread.thread_url == canonical_url).first()

    if meta is None:
        meta = db.query(CachedThread).filter(CachedThread.thread_url == alt_show_url).first()
        if meta is not None:
            _migrate_cache_key_if_needed(db, alt_show_url, canonical_url)
            meta = db.query(CachedThread).filter(CachedThread.thread_url == canonical_url).first()

    need_refresh = True
    if meta and (now - meta.fetched_at < THREAD_CACHE_TTL):
        need_refresh = False

    if need_refresh:
        posts = fetch_posts_from_thread(canonical_url)
        _save_thread_posts_to_cache(db, canonical_url, list(posts))
        cached_rows = _load_thread_posts_from_cache(db, canonical_url)
    else:
        try:
            meta.last_accessed_at = now
            db.commit()
        except Exception:
            db.rollback()
        cached_rows = _load_thread_posts_from_cache(db, canonical_url)

    result: List[object] = []
    for r in cached_rows:
        result.append(
            SimpleNamespace(
                post_no=r.post_no,
                posted_at=r.posted_at,
                body=r.body,
                anchors=parse_anchors_csv(r.anchors),
            )
        )
    return result

