"""
The session cookie (I-25). On sign-in the server sets oauth_token, the session's JWT, as an
HttpOnly cookie with SameSite=Strict, so scripts in the page cannot read it: the browser
sends it with every request to MageML, and the API, websockets, the terminal and downloads
read the user from it. A second cookie, oauth_token_expires, holds only the session's expiry
for the pages to know whether one is signed in. Signing out clears both.

The pages used to set oauth_token themselves, readable by any script, with the page's host
as its Domain; that copy is cleared when the server sets its own.
"""
from datetime import datetime, timezone
from typing import Dict, Optional

from mage_ai.server.api.constants import COOKIE_OAUTH_TOKEN

COOKIE_EXPIRES = 'oauth_token_expires'


def _secure(handler) -> bool:
    request = handler.request
    forwarded = (request.headers.get('X-Forwarded-Proto') or '').split(',')[0].strip()
    return request.protocol == 'https' or forwarded == 'https'


def _clear_page_cookie(handler) -> None:
    """The copy the pages set, with Domain=<host>; a host-only cookie of the same name is
    another cookie, and both would be sent."""
    host = (handler.request.host or '').split(':')[0]
    if host:
        handler.clear_cookie(COOKIE_OAUTH_TOKEN, path='/', domain=host)


def set_session(handler, token: str, expires: Optional[float]) -> None:
    secure = _secure(handler)
    expires_at = datetime.fromtimestamp(expires, tz=timezone.utc) if expires else None
    _clear_page_cookie(handler)
    handler.set_cookie(
        COOKIE_OAUTH_TOKEN, token, path='/', expires=expires_at, httponly=True,
        samesite='Strict', secure=secure,
    )
    handler.set_cookie(
        COOKIE_EXPIRES, str(int(expires)) if expires else '', path='/', expires=expires_at,
        samesite='Strict', secure=secure,
    )


def clear_session(handler) -> None:
    _clear_page_cookie(handler)
    handler.clear_cookie(COOKIE_OAUTH_TOKEN, path='/')
    handler.clear_cookie(COOKIE_EXPIRES, path='/')


def apply(handler, resource: str, action: str, response: Dict) -> None:
    """After a sessions request: a sign-in sets the cookies, a sign-out clears them."""
    if resource != 'sessions' or not isinstance(response, dict) or response.get('error'):
        return
    if action == 'create':
        session = response.get('session') or {}
        token = session.get('token')
        if token:
            from mage_ai.authentication.oauth2 import decode_token

            try:
                expires = decode_token(token).get('expires')
            except Exception:
                return
            set_session(handler, token, expires)
    elif action in ('update', 'delete'):
        clear_session(handler)
