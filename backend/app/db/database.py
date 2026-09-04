from sqlalchemy import create_engine
from app.core.config import Settings
from sqlalchemy.orm import DeclarativeBase, sessionmaker

_connect_args = (
    {"connect_timeout": 5} if Settings.DATABASE_URL.startswith("mysql") else {}
)

engine = create_engine(
    Settings.DATABASE_URL,
    pool_pre_ping=True,
    connect_args=_connect_args,
)

sessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False
)

class Base(DeclarativeBase):
    pass

def get_db():
    db = sessionLocal()

    try:
        yield db
    finally:
        db.close()
