from fastapi import FastAPI
from app.core.config import Settings
import logging
from app.core.logger import setup_logging
from contextlib import asynccontextmanager
from app.api.v1.health import router as health_router
from app.api import auth, analysis

setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting Devflo API")

    yield
    logger.info("shutting down Devflo API")

app = FastAPI(title=Settings.APP_NAME, version=Settings.APP_VERSION, lifespan=lifespan)
app.include_router(health_router)
app.include_router(auth.router)
app.include_router(analysis.router)

@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"app": Settings.APP_NAME, "version": Settings.APP_VERSION}
