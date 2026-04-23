import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth.jwt_handler import TokenData, get_current_user
from app.routes import admin as admin_router
from app.routes import auth as auth_router
from app.routes import filters as filters_router
from app.routes import providers as providers_router
from app.routes import schedules as schedules_router
from app.utils.logging_config import configure_logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    debug = os.getenv("DEBUG", "false").lower() == "true"
    configure_logging(debug=debug)
    logger.info("Application starting up")

    from app.database import init_db
    init_db()
    logger.info("Database tables verified")

    from run_migrations import run_all_migrations
    run_all_migrations()

    from app.scheduler import start_scheduler
    start_scheduler()
    logger.info("Scheduler started")

    yield

    from app.scheduler import stop_scheduler
    stop_scheduler()
    logger.info("Application shutting down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="vodstrm",
        lifespan=lifespan,
        docs_url="/docs" if os.getenv("DEBUG", "false").lower() == "true" else None,
        redoc_url=None,
    )

    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(BASE_DIR, "static")),
        name="static",
    )

    app.include_router(auth_router.router)
    app.include_router(admin_router.router)
    app.include_router(filters_router.router)
    app.include_router(providers_router.router)
    app.include_router(schedules_router.router)

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, current_user: TokenData = Depends(get_current_user)):
        from app.routes.auth import _admin_exists
        if not _admin_exists():
            return RedirectResponse("/setup", status_code=302)
        return templates.TemplateResponse(
            "index.html", {"request": request, "current_user": current_user}
        )

    @app.exception_handler(Exception)
    async def redirect_unauthorized(request: Request, exc: Exception):
        from fastapi import HTTPException
        if isinstance(exc, HTTPException) and exc.status_code == 302:
            location = exc.headers.get("Location", "/login")
            return RedirectResponse(location, status_code=302)
        raise exc

    return app


app = create_app()
