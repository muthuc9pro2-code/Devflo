from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from app.core.config import Settings
import logging
from app.core.logger import setup_logging
from app.core.processing_config import MAX_ANALYSIS_REQUEST_BODY_BYTES
from app.core.request_body_limit import RequestBodyLimitMiddleware
from contextlib import asynccontextmanager
from app.api.v1.health import router as health_router
from app.api import auth, analysis, analysis_stream

setup_logging()
logger = logging.getLogger(__name__)

_SERVICE_UNAVAILABLE_DETAIL = "Devflo is temporarily unavailable. Please try again."
_SERVICE_UNAVAILABLE_CODE = "service_unavailable"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting Devflo API")

    yield
    logger.info("shutting down Devflo API")

app = FastAPI(title=Settings.APP_NAME, version=Settings.APP_VERSION, lifespan=lifespan)

app.add_middleware(
    RequestBodyLimitMiddleware,
    path="/analysis/upload",
    max_body_size=MAX_ANALYSIS_REQUEST_BODY_BYTES,
)


@app.exception_handler(OperationalError)
async def handle_database_unavailable(request: Request, exc: OperationalError) -> JSONResponse:
    logger.error("Database unavailable for %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=503,
        content={
            "detail": _SERVICE_UNAVAILABLE_DETAIL,
            "code": _SERVICE_UNAVAILABLE_CODE,
        },
    )


app.include_router(health_router)
app.include_router(auth.router)
app.include_router(analysis.router)
app.include_router(analysis_stream.router)

@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"app": Settings.APP_NAME, "version": Settings.APP_VERSION}

