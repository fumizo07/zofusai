# 001
# routers/kb_diary_api.py
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from db import get_db


router = APIRouter(prefix="/kb/api", tags=["kb-diary"])


# ---------- 設定 ----------
FETCH_TIMEOUT_SEC = 12.0
CACHE_TTL_MIN = 10  # 同一人物の「最新チェック」をこの分数は再取得しない（負荷対策）


# ---------- 入出力 ----------
class DiaryStatusItemIn(BaseModel):
  person_id: int = Field(..., ge=1)
  diary_url: str = Field(..., min_length=1)


class DiaryStatusRequest(BaseModel):
  items: List[DiaryStatusItemIn] = Field(default_factory=list)


class DiaryStatusItemOut(BaseModel):
  person_id: int
  diary_key: str = ""
  is_new: bool = False
  checked: bool = False  # True=今回実際に取得した / False=キャッシュ利用


class DiarySeenRequest(BaseModel):
  person_id: int = Field(..., ge=1)
  diary_key: str = ""  # 空なら「latest_key を seen にする」


# ---------- 取得→キー化 ----------
_DIARY_KEY_PATTERNS = [
  # ありがちなID類を拾う（サイト差があるので複数）
  re.compile(r"(?:diary|blog)[^0-9]{0,20}(\d{4,})", re.IGNORECASE),
  re.compile(r"(?:entry|article|post)[^0-9]{0,20}(\d{4,})", re.IGNORECASE),
  re.compile(r"diary[^\"']{0,200}href=[\"'][^\"']*(\d{4,})", re.IGNORECASE),
]

def _make_fallback_key(html: str) -> str:
  # 落とし穴：サイトごとに構造が違うので、最終手段は HTML の先頭部分をハッシュ
  head = (html or "")[:50000]
  head = re.sub(r"\s+", " ", head)
  return hashlib.sha1(head.encode("utf-8", errors="ignore")).hexdigest()

def extract_diary_key(html: str) -> str:
  s = html or ""
  for pat in _DIARY_KEY_PATTERNS:
    m = pat.search(s)
    if m and m.group(1):
      # "IDっぽい数字" をキー扱い
      return f"id:{m.group(1)}"
  return f"sha1:{_make_fallback_key(s)}"

async def fetch_diary_key(diary_url: str) -> str:
  url = (diary_url or "").strip()
  if not url:
    return ""

  headers = {
    "User-Agent": "Mozilla/5.0 (compatible; PersonalSearchKB/1.0; +https://example.invalid)"
  }

  async with httpx.AsyncClient(follow_redirects=True, timeout=FETCH_TIMEOUT_SEC, headers=headers) as client:
    r = await client.get(url)
    if r.status_code != 200:
      return ""
    html = r.text or ""
    return extract_diary_key(html)


# ---------- DBユーティリティ ----------
def utcnow() -> datetime:
  return datetime.now(timezone.utc)

def get_state(db: Session, person_id: int) -> Optional[Dict[str, Any]]:
  row = db.execute(
    text("""
      SELECT person_id, latest_key, latest_checked_at, seen_key, seen_at
      FROM kb_diary_state
      WHERE person_id = :pid
    """),
    {"pid": int(person_id)},
  ).mappings().first()
  return dict(row) if row else None

def upsert_latest(db: Session, person_id: int, latest_key: str, checked_at: datetime) -> None:
  db.execute(
    text("""
      INSERT INTO kb_diary_state (person_id, latest_key, latest_checked_at)
      VALUES (:pid, :lkey, :cat)
      ON CONFLICT (person_id)
      DO UPDATE SET
        latest_key = EXCLUDED.latest_key,
        latest_checked_at = EXCLUDED.latest_checked_at
    """),
    {"pid": int(person_id), "lkey": latest_key, "cat": checked_at},
  )

def upsert_seen(db: Session, person_id: int, seen_key: str, seen_at: datetime) -> None:
  db.execute(
    text("""
      INSERT INTO kb_diary_state (person_id, seen_key, seen_at)
      VALUES (:pid, :skey, :sat)
      ON CONFLICT (person_id)
      DO UPDATE SET
        seen_key = EXCLUDED.seen_key,
        seen_at = EXCLUDED.seen_at
    """),
    {"pid": int(person_id), "skey": seen_key, "sat": seen_at},
  )


# ---------- API ----------
@router.post("/diary_status")
async def diary_status(req: DiaryStatusRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
  """
  items: [{person_id, diary_url}]
  戻り: items: [{person_id, diary_key, is_new, checked}]
  仕様:
    - 初回（seen_keyが無い）は latest_key を seen_key に自動コピーして is_new=False（静かな開始）
    - 10分以内にチェック済みならキャッシュ使用（checked=False）
  """
  out: List[DiaryStatusItemOut] = []

  now = utcnow()
  ttl = timedelta(minutes=CACHE_TTL_MIN)

  # 変な入力は落とさずスキップ（UI側の不具合耐性）
  safe_items = []
  for it in (req.items or []):
    pid = int(it.person_id)
    url = (it.diary_url or "").strip()
    if pid <= 0 or not url:
      continue
    safe_items.append((pid, url))

  # 並列取得（必要なものだけ）
  # 取得要否判定を先にDBで行う
  need_fetch: List[tuple[int, str]] = []
  cache_map: Dict[int, Dict[str, Any]] = {}

  for pid, url in safe_items:
    st = get_state(db, pid)
    if st and st.get("latest_checked_at"):
      try:
        last = st["latest_checked_at"]
        # last が naive の場合もあるので保険
        if last.tzinfo is None:
          last = last.replace(tzinfo=timezone.utc)
      except Exception:
        last = None
      if last and (now - last) <= ttl and st.get("latest_key"):
        cache_map[pid] = st
        continue

    need_fetch.append((pid, url))

  fetched_keys: Dict[int, str] = {}
  checked_map: Dict[int, bool] = {}

  async def _one(pid: int, url: str) -> None:
    try:
      k = await fetch_diary_key(url)
      fetched_keys[pid] = k or ""
      checked_map[pid] = True
    except Exception:
      fetched_keys[pid] = ""
      checked_map[pid] = True

  if need_fetch:
    await httpx.AsyncClient().aclose()  # no-op; 環境によっては警告回避
    # gather
    import asyncio
    await asyncio.gather(*[_one(pid, url) for pid, url in need_fetch])

  # DB更新（latest）
  for pid, _url in need_fetch:
    k = fetched_keys.get(pid, "") or ""
    upsert_latest(db, pid, k, now)

  db.commit()

  # 最終判定（seen_keyが無ければ静かに初期化）
  for pid, _url in safe_items:
    st = get_state(db, pid)  # 更新後に取り直し
    latest_key = (st.get("latest_key") if st else "") or ""
    seen_key = (st.get("seen_key") if st else "") or ""

    if latest_key and not seen_key:
      # 初回は「NEWを出さない」：最新を既読として登録
      upsert_seen(db, pid, latest_key, now)
      db.commit()
      seen_key = latest_key

    is_new = bool(latest_key and seen_key and latest_key != seen_key)

    out.append(DiaryStatusItemOut(
      person_id=pid,
      diary_key=latest_key or "",
      is_new=is_new,
      checked=bool(checked_map.get(pid, False)),
    ))

  return {"ok": True, "items": [x.model_dump() for x in out]}


@router.post("/diary_seen")
def diary_seen(req: DiarySeenRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
  """
  person_id を既読化。
  diary_key が空なら、DBにある latest_key を seen_key にコピー。
  """
  pid = int(req.person_id)
  if pid <= 0:
    return {"ok": False, "error": "invalid_person_id"}

  st = get_state(db, pid) or {}
  latest_key = (st.get("latest_key") or "")
  seen_key = (req.diary_key or "").strip() or latest_key

  if not seen_key:
    return {"ok": False, "error": "no_key"}

  upsert_seen(db, pid, seen_key, utcnow())
  db.commit()
  return {"ok": True}
