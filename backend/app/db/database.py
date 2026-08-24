from sqlalchemy import create_engine
from app.core.config import Settings
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# pool_pre_ping: a stale pooled MySQL connection (e.g. the server restarted
# or an idle connection was dropped) is detected with a lightweight ping
# when checked out of the pool and transparently replaced, instead of
# surfacing as a confusing failure on whatever query happened to run next.
# connect_timeout: MySQL-only (PyMySQL-specific connect_args key - would be
# rejected by sqlite3.connect(), which tests' DATABASE_URL uses) - bounds
# how long a NEW connection attempt can hang against an unreachable MySQL
# server, so a DB outage fails fast and predictably instead of hanging an
# interactive API request.
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

