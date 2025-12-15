
from sqlalchemy import Column, Integer, Text, DateTime, UniqueConstraint

from app.db.session import Base

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

    # ①（高速化）用：正規化カラム（無くても動くが、入れると速い）
    body_norm = Column(Text, nullable=True)
    thread_title_norm = Column(Text, nullable=True)
    tags_norm = Column(Text, nullable=True)


class ThreadMeta(Base):
    """
    スレッド単位のメタ情報（自分用ラベルなど）
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
