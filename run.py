"""
Development entry point for PyCharm / IDE use.
Run this file directly to start the app with auto-reload enabled.
"""
import os

import uvicorn
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    port = int(os.getenv("APP_PORT", "2112"))
    debug = os.getenv("DEBUG", "false").lower() == "true"

    in_docker = os.path.exists("/app") and os.path.isfile("/app/run.py")
    host = "0.0.0.0" if in_docker else "127.0.0.1"

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=True,
        log_level="debug" if debug else "info",
    )
