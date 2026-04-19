import os

_DOCKER_ROOT = "/app"


def is_docker() -> bool:
    return os.path.exists(_DOCKER_ROOT) and os.path.isfile(os.path.join(_DOCKER_ROOT, "run.py"))


def project_root() -> str:
    if is_docker():
        return _DOCKER_ROOT
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def resolve_path(*parts: str) -> str:
    raw = os.path.join(*parts)
    if os.path.isabs(raw):
        return raw
    return os.path.join(project_root(), raw)
