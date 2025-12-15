from typing import Dict, List
from collections import defaultdict
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.config import THREAD_CACHE_TTL, MAX_CACHED_THREADS
from app.db.models import CachedPost, CachedThread
from app.services.text_utils import parse_anchors_csv

from scraper import fetch_posts_from_thread


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
        meta = CachedThread(
            thread_url=thread_url,
            fetched_at=now,
            last_accessed_at=now,
        )
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
    thread_url = (thread_url or "").strip()
    if not thread_url:
        return []

    now = datetime.utcnow()
    meta = db.query(CachedThread).filter(CachedThread.thread_url == thread_url).first()

    need_refresh = True
    if meta:
        if now - meta.fetched_at < THREAD_CACHE_TTL:
            need_refresh = False

    if need_refresh:
        posts = fetch_posts_from_thread(thread_url)
        _save_thread_posts_to_cache(db, thread_url, list(posts))
        cached_rows = _load_thread_posts_from_cache(db, thread_url)
    else:
        try:
            meta.last_accessed_at = now
            db.commit()
        except Exception:
            db.rollback()
        cached_rows = _load_thread_posts_from_cache(db, thread_url)

    result = []
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
