"""Shared Jinja template environment for application pages."""
import os

from fastapi.templating import Jinja2Templates

from app.utils.version import get_local_version


_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


def create_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory=_TEMPLATE_DIR)
    templates.env.globals["app_version"] = get_local_version()
    return templates