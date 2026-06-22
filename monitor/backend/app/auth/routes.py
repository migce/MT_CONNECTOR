"""
Auth routes: login, refresh, me, change-password.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    create_access_token,
    create_refresh_token,
    hash_password,
    hash_refresh_token,
    verify_password,
)
from app.auth.dependencies import get_current_user
from app.auth.schemas import (
    LoginRequest,
    PasswordChange,
    RefreshRequest,
    TokenResponse,
    UserResponse,
)
from app.config import get_settings
from app.db import get_session
from app.db.models import RefreshToken, User

router = APIRouter()


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    result = await session.execute(select(User).where(User.username == body.username))
    user = result.scalar_one_or_none()
    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account disabled")

    access = create_access_token(str(user.id), user.role)
    raw_refresh, refresh_hash = create_refresh_token()

    settings = get_settings()
    token = RefreshToken(
        user_id=user.id,
        token_hash=refresh_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days),
    )
    session.add(token)
    user.last_login = datetime.now(timezone.utc)
    await session.commit()

    return TokenResponse(access_token=access, refresh_token=raw_refresh)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    body: RefreshRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    token_hash = hash_refresh_token(body.refresh_token)
    result = await session.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    )
    token = result.scalar_one_or_none()
    if token is None or token.expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")

    # rotate: delete old, create new
    await session.delete(token)

    user = await session.get(User, token.user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")

    access = create_access_token(str(user.id), user.role)
    raw_refresh, new_hash = create_refresh_token()

    settings = get_settings()
    new_token = RefreshToken(
        user_id=user.id,
        token_hash=new_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.refresh_token_expire_days),
    )
    session.add(new_token)
    await session.commit()

    return TokenResponse(access_token=access, refresh_token=raw_refresh)


@router.post("/logout")
async def logout(
    body: RefreshRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    token_hash = hash_refresh_token(body.refresh_token)
    await session.execute(delete(RefreshToken).where(RefreshToken.token_hash == token_hash))
    await session.commit()
    return {"ok": True}


@router.get("/me", response_model=UserResponse)
async def me(user: Annotated[User, Depends(get_current_user)]):
    return user


@router.post("/me/password")
async def change_password(
    body: PasswordChange,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    if not verify_password(body.old_password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Wrong current password")
    user.password_hash = hash_password(body.new_password)
    session.add(user)
    await session.commit()
    return {"ok": True}
