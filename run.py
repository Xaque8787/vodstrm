"""
Development entry point for PyCharm / IDE use.
Run this file directly to start the app with auto-reload enabled.
"""
import uvicorn
from app.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_reload,
        log_level="debug" if settings.debug else "info",
    )
