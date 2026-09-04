"""Authentication guard for money-moving Connector APIs."""

from __future__ import annotations

import hmac

from fastapi import HTTPException, Request, status

from src.config import get_settings


async def require_internal_api(request: Request) -> str:
    """Require the configured internal bearer token.

    An empty server-side token deliberately disables the protected API instead
    of accidentally making it public.
    """
    expected = get_settings().internal_api_token.strip()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Protected Connector API is disabled: INTERNAL_API_TOKEN is not configured.",
        )

    authorization = request.headers.get("authorization", "")
    scheme, _, supplied = authorization.partition(" ")
    if scheme.lower() != "bearer" or not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal API credentials.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return "internal-service"
