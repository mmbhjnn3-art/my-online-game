"""Entry point for deployment platforms that run `uvicorn main:app`."""
from server import app

__all__ = ["app"]
