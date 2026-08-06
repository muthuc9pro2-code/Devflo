from sqlalchemy import create_engine
from app.core.config import Settings
from sqlalchemy.orm import DeclarativeBase, sessionmaker

engine = create_engine(Settings.DATABASE_URL)

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

