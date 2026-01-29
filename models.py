# 010
# models.py
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    Text,
    DateTime,
    UniqueConstraint,
    ForeignKey,
    JSON,
    Boolean,
)
from db import Base


class ThreadPost(Base):
    __tablename__ = "thread_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)
    thread_title = Column(Text, nullable=True)
    post_no = Column(Integer, nullable=True, index=True)

    posted_at = Column(Text, nullable=True)
    posted_at_dt = Column(DateTime, nullable=True, index=True)

    body = Column(Text, nullable=False)
    anchors = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)
    memo = Column(Text, nullable=True)

    body_norm = Column(Text, nullable=True, index=True)
    thread_title_norm = Column(Text, nullable=True, index=True)
    tags_norm = Column(Text, nullable=True, index=True)


class ThreadMeta(Base):
    __tablename__ = "thread_meta"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, unique=True, index=True)
    label = Column(Text, nullable=True)


class CachedThread(Base):
    __tablename__ = "cached_threads"

    thread_url = Column(Text, primary_key=True)
    fetched_at = Column(DateTime, nullable=False)
    last_accessed_at = Column(DateTime, nullable=False)


class CachedPost(Base):
    __tablename__ = "cached_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)
    post_no = Column(Integer, nullable=True, index=True)
    posted_at = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    anchors = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("thread_url", "post_no", name="uq_cached_posts_thread_postno"),
    )


class ExternalSearchHistory(Base):
    __tablename__ = "external_search_history"

    id = Column(Integer, primary_key=True, index=True)

    key = Column(Text, nullable=False, unique=True, index=True)

    area = Column(Text, nullable=False, index=True)
    period = Column(Text, nullable=False, index=True)
    board_category = Column(Text, nullable=True, index=True)
    board_id = Column(Text, nullable=True, index=True)
    keyword = Column(Text, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    hit_count = Column(Integer, nullable=False, default=1)

    __table_args__ = (
        UniqueConstraint("key", name="uq_external_search_history_key"),
    )


# ============================================================
# KB
# ============================================================
class KBRegion(Base):
    __tablename__ = "kb_regions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False, unique=True, index=True)
    name_norm = Column(Text, nullable=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class KBStore(Base):
    __tablename__ = "kb_stores"

    id = Column(Integer, primary_key=True, index=True)
    region_id = Column(Integer, ForeignKey("kb_regions.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(Text, nullable=False, index=True)
    name_norm = Column(Text, nullable=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("region_id", "name", name="uq_kb_stores_region_name"),
    )


class KBPerson(Base):
    __tablename__ = "kb_persons"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("kb_stores.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(Text, nullable=False, index=True)

    age = Column(Integer, nullable=True, index=True)
    height_cm = Column(Integer, nullable=True, index=True)
    cup = Column(Text, nullable=True, index=True)

    bust_cm = Column(Integer, nullable=True)
    waist_cm = Column(Integer, nullable=True)
    hip_cm = Column(Integer, nullable=True)

    services = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)

    # ★追加：URL
    url = Column(Text, nullable=True)

    # ★追加：画像URL（複数）: ["https://...jpg", "https://...png", ...]
    image_urls = Column(JSON, nullable=True)

    memo = Column(Text, nullable=True)

    # 将来用の正規化列
    name_norm = Column(Text, nullable=True, index=True)
    services_norm = Column(Text, nullable=True, index=True)
    tags_norm = Column(Text, nullable=True, index=True)

    # ★追加：URLの正規化
    url_norm = Column(Text, nullable=True, index=True)

    memo_norm = Column(Text, nullable=True, index=True)

    # フリーワード検索のためのまとめ列
    search_norm = Column(Text, nullable=True, index=True)

    # ★追加：お気に入り（DB: favorite boolean default false）
    favorite = Column(Boolean, nullable=False, default=False, index=True)

    # 既存の diary_* は残します（互換性のため）
    diary_last_entry_at = Column(DateTime, nullable=True, index=True)
    diary_last_entry_key = Column(Text, nullable=True, index=True)

    diary_checked_at = Column(DateTime, nullable=True, index=True)

    diary_seen_at = Column(DateTime, nullable=True, index=True)
    diary_seen_entry_key = Column(Text, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("store_id", "name", name="uq_kb_persons_store_name"),
    )


# ============================================================
# KB 写メ日記追跡状態（★追加）
# - 既存 kb_persons にカラム追加せず、新規テーブルで安全に管理する
# - 仕様B（追跡ON直後はNEWを出さない）:
#   track_enabled をONにするタイミングで latest_entry_* を取得し、
#   seen_* を latest_* と同値にして基準点を作る（静かな運用）
# ============================================================
class KBDiaryState(Base):
    __tablename__ = "kb_diary_states"

    id = Column(Integer, primary_key=True, index=True)

    person_id = Column(
        Integer,
        ForeignKey("kb_persons.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,
    )

    # 追跡ON/OFF（チェックボックス）
    track_enabled = Column(Boolean, nullable=False, default=False, index=True)

    # 最後に取得できた「最新日記」の情報
    latest_entry_at = Column(DateTime, nullable=True, index=True)
    latest_entry_key = Column(Text, nullable=True, index=True)

    # ユーザーが確認済みにした基準点（NEWを消す）
    seen_at = Column(DateTime, nullable=True, index=True)
    seen_entry_key = Column(Text, nullable=True, index=True)

    # 最後に外部へ取りに行った時刻（インターバル判定）
    fetched_at = Column(DateTime, nullable=True, index=True)

    # 取得失敗時の抑制・デバッグ用
    last_error = Column(Text, nullable=True)
    error_at = Column(DateTime, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", name="uq_kb_diary_states_person_id"),
    )


class KBVisit(Base):
    __tablename__ = "kb_visits"

    id = Column(Integer, primary_key=True, index=True)
    person_id = Column(Integer, ForeignKey("kb_persons.id", ondelete="CASCADE"), nullable=False, index=True)

    visited_at = Column(DateTime, nullable=True, index=True)

    # DBは start_time/end_time を「分（int）」で持つ前提に統一
    start_time = Column(Integer, nullable=True)  # 分（0-1439）
    end_time = Column(Integer, nullable=True)    # 分（0-1439）
    duration_min = Column(Integer, nullable=True, index=True)

    rating = Column(Integer, nullable=True, index=True)
    memo = Column(Text, nullable=True)

    price_items = Column(JSON, nullable=True)
    total_yen = Column(Integer, nullable=False, default=0)

    # フリーワード検索用
    search_norm = Column(Text, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


# ============================================================
# KB 料金テンプレ（★追加）
# - 店舗ごと
# - items は [{label: str, amount: int}, ...]
# ============================================================
class KBPriceTemplate(Base):
    __tablename__ = "kb_price_templates"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("kb_stores.id", ondelete="CASCADE"), nullable=False, index=True)

    name = Column(Text, nullable=False, index=True)
    items = Column(JSON, nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("store_id", "name", name="uq_kb_price_templates_store_name"),
    )
