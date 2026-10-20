"""Jinja templates compiled once per text."""
from functools import lru_cache

from jinja2 import Template


@lru_cache(maxsize=256)
def compiled(text: str) -> Template:
    """
    The template for this text, compiled the first time it is seen. Compiling took most of
    a render: a run read the project's metadata.yaml as a template a dozen times, about a
    quarter of a short run on a warm worker. Rendering is still done on each call, so the
    values a template reads (environment variables, secrets) are the current ones.
    """
    return Template(text)
