from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class SentimentPost(Base, TimestampMixin):
    """Normalized social sentiment post cached for research workflows."""

    __tablename__ = "sentiment_posts"
    __table_args__ = (
        UniqueConstraint("source", "source_post_id", name="uq_sentiment_source_post"),
        Index("ix_sentiment_symbol_published", "symbol", "published_at"),
        Index("ix_sentiment_source_symbol", "source", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_post_id: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str | None] = mapped_column(String(500))
    content: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(String(128))
    published_at: Mapped[datetime | None] = mapped_column(DateTime)
    url: Mapped[str | None] = mapped_column(String(1000))
    reply_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    like_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comment_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sentiment_score: Mapped[float | None] = mapped_column(Float)
    sentiment_label: Mapped[str | None] = mapped_column(String(20))
    keywords_json: Mapped[str | None] = mapped_column(Text)
    raw_json: Mapped[str | None] = mapped_column(Text)


class SentimentThread(Base, TimestampMixin):
    """Source-level discussion thread before symbol expansion."""

    __tablename__ = "sentiment_threads"
    __table_args__ = (
        UniqueConstraint("source", "source_thread_id", name="uq_sentiment_source_thread"),
        Index("ix_sentiment_thread_source_published", "source", "published_at"),
        Index("ix_sentiment_thread_source_last_reply", "source", "last_reply_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_thread_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str | None] = mapped_column(String(500))
    content: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(String(128))
    published_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_reply_at: Mapped[datetime | None] = mapped_column(DateTime)
    url: Mapped[str | None] = mapped_column(String(1000))
    reply_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comment_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sentiment_score: Mapped[float | None] = mapped_column(Float)
    sentiment_label: Mapped[str | None] = mapped_column(String(20))
    symbols_json: Mapped[str | None] = mapped_column(Text)
    keywords_json: Mapped[str | None] = mapped_column(Text)
    raw_json: Mapped[str | None] = mapped_column(Text)


class SentimentMention(Base, TimestampMixin):
    """Evidence-backed mapping from a source document to an A-share symbol."""

    __tablename__ = "sentiment_mentions"
    __table_args__ = (
        UniqueConstraint("source", "source_thread_id", "symbol", name="uq_sentiment_mention"),
        Index("ix_sentiment_mention_symbol", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_thread_id: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    match_method: Mapped[str] = mapped_column(String(32), nullable=False, default="legacy")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    evidence: Mapped[str | None] = mapped_column(String(500))


class SentimentAnalysis(Base, TimestampMixin):
    """Versioned sentiment output for a source item and optional symbol scope."""

    __tablename__ = "sentiment_analyses"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "source_item_id",
            "symbol",
            "model_version",
            name="uq_sentiment_analysis_version",
        ),
        Index("ix_sentiment_analysis_symbol", "symbol", "analyzed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_item_id: Mapped[str] = mapped_column(String(160), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    score: Mapped[float | None] = mapped_column(Float)
    label: Mapped[str | None] = mapped_column(String(20))
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    evidence_json: Mapped[str | None] = mapped_column(Text)
    keywords_json: Mapped[str | None] = mapped_column(Text)
    analyzed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)


class SentimentFocusSnapshot(Base, TimestampMixin):
    """Sanitized target resolution used by scheduled sentiment ingestion."""

    __tablename__ = "sentiment_focus_snapshots"
    __table_args__ = (
        Index("ix_sentiment_focus_snapshot_key_captured", "snapshot_key", "captured_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_key: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    captured_at: Mapped[datetime | None] = mapped_column(DateTime)
    symbols_json: Mapped[str] = mapped_column(Text, nullable=False)
    provenance_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(500))
