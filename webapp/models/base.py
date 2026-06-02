"""SQLAlchemy declarative base, engine, and session factory."""

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from config import Config


class Base(DeclarativeBase):
    pass


engine = create_engine(
    Config.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_db() -> Session:
    """Get a database session. Caller must close it."""
    return SessionLocal()
