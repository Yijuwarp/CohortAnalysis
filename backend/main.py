"""
Short summary: exposes the ASGI app for external runners.
"""
if __package__:
    from .app.main import app
else:
    from app.main import app

__all__ = ["app"]
