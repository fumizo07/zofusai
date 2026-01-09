from datetime import datetime

from sqlalchemy import Column, Integer, Text, DateTime, UniqueConstraint, ForeignKey, JSON
from db import Base


class ThreadPost(Base):
    __tablename__ = "thread_posts"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, index=True)
    thread_title = Column(Text, nullable=True)
    post_no = Column(Integer, nullable=True, index=True)

    # 既存互換（そのまま残す）
    posted_at = Column(Text, nullable=True)

    # ★追加：検索・並び替え精度のための DateTime 正規化列
    posted_at_dt = Column(DateTime, nullable=True, index=True)

    body = Column(Text, nullable=False)
    anchors = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)
    memo = Column(Text, nullable=True)

    # ★追加：内部検索の「揺らぎ対応」用（NFKC + ひらがな化 + lower 等）
    body_norm = Column(Text, nullable=True, index=True)
    thread_title_norm = Column(Text, nullable=True, index=True)
    tags_norm = Column(Text, nullable=True, index=True)


class ThreadMeta(Base):
    """
    スレッド単位のメタ情報（自分用ラベルなど）を持たせるテーブル
    """
    __tablename__ = "thread_meta"

    id = Column(Integer, primary_key=True, index=True)
    thread_url = Column(Text, nullable=False, unique=True, index=True)
    label = Column(Text, nullable=True)


class CachedThread(Base):
    """
    スレ単位のキャッシュ管理（いつ取得したか、いつ使ったか）
    """
    __tablename__ = "cached_threads"

    thread_url = Column(Text, primary_key=True)
    fetched_at = Column(DateTime, nullable=False)
    last_accessed_at = Column(DateTime, nullable=False)


class CachedPost(Base):
    """
    スレの各レス（全文キャッシュ）
    """
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
    """
    外部検索（スレ検索）の履歴：DB永続化
    - key でユニーク（同じ条件なら1件にまとまる）
    - last_seen_at で「最近使った順」
    """
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
# ここから「知った情報を整理する（KB）」系
# ============================================================

class KBRegion(Base):
    __tablename__ = "kb_regions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False, unique=True, index=True)

    # ゆらぎ検索用（将来用：無くても動くが、入れておく）
    name_norm = Column(Text, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class KBStore(Base):
    __tablename__ = "kb_stores"

    id = Column(Integer, primary_key=True, index=True)
    region_id = Column(Integer, ForeignKey("kb_regions.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(Text, nullable=False, index=True)

    # ゆらぎ検索用（将来用）
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

    # ★追加：年齢
    age = Column(Integer, nullable=True, index=True)

    height_cm = Column(Integer, nullable=True, index=True)

    # ★追加：カップ（例: "B" / "C"）
    cup = Column(Text, nullable=True, index=True)

    bust_cm = Column(Integer, nullable=True)
    waist_cm = Column(Integer, nullable=True)
    hip_cm = Column(Integer, nullable=True)

    # ★追加：サービス（複数OK。まずはTextで運用）
    # 例: "NN可, 写メ日記, デート" / あるいは改行区切りなど
    services = Column(Text, nullable=True)

    tags = Column(Text, nullable=True)       # カンマ区切り（まずはシンプル）
    memo = Column(Text, nullable=True)       # 人の固定メモ（プロフィール的なやつ）

    # ★追加：ゆらぎ検索用（将来用：NFKC + ひらがな化 + lower 等）
    name_norm = Column(Text, nullable=True, index=True)
    services_norm = Column(Text, nullable=True, index=True)
    tags_norm = Column(Text, nullable=True, index=True)
    memo_norm = Column(Text, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("store_id", "name", name="uq_kb_persons_store_name"),
    )


class KBVisit(Base):
    """
    利用ログ（自分用口コミ）
    - 星評価(1-5)
    - 料金項目（JSON配列）と合計
    - ★追加：開始/終了時刻と利用分数
    """
    __tablename__ = "kb_visits"

    id = Column(Integer, primary_key=True, index=True)
    person_id = Column(Integer, ForeignKey("kb_persons.id", ondelete="CASCADE"), nullable=False, index=True)

    # 既存：日付入力を DateTime で保持（テンプレ側で YYYY-MM-DD 表示にする）
    visited_at = Column(DateTime, nullable=True, index=True)

    # ★追加：開始/終了（"HH:MM"想定）
    start_time = Column(Text, nullable=True)
    end_time = Column(Text, nullable=True)

    # ★追加：利用分（分単位）
    duration_min = Column(Integer, nullable=True, index=True)

    rating = Column(Integer, nullable=True, index=True)  # 1〜5
    memo = Column(Text, nullable=True)                   # 口コミ本文

    # 例: [{"label":"基本料金","amount":12000},{"label":"オプション","amount":3000}]
    price_items = Column(JSON, nullable=True)
    total_yen = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
