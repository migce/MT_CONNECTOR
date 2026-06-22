"""
Admin routes: user CRUD + account binding.
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import hash_password
from app.auth.dependencies import require_admin
from app.auth.schemas import (
    AccountBind,
    AccountBindResponse,
    UserCreate,
    UserResponse,
    UserUpdate,
)
from app.db import get_session
from app.db.models import User, UserAccount

router = APIRouter()


# ── List users ───────────────────────────────────────────────────────
@router.get("", response_model=list[UserResponse])
async def list_users(
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    result = await session.execute(select(User).order_by(User.created_at))
    return result.scalars().all()


# ── Create user ──────────────────────────────────────────────────────
@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    body: UserCreate,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    # check uniqueness
    existing = await session.execute(
        select(User).where((User.username == body.username) | (User.email == body.email))
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username or email already taken")

    user = User(
        username=body.username,
        email=body.email,
        password_hash=hash_password(body.password),
        role=body.role,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


# ── Get user ─────────────────────────────────────────────────────────
@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: uuid.UUID,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# ── Update user ──────────────────────────────────────────────────────
@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: uuid.UUID,
    body: UserUpdate,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if body.email is not None:
        user.email = body.email
    if body.password is not None:
        user.password_hash = hash_password(body.password)
    if body.role is not None:
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active

    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


# ── Delete user ──────────────────────────────────────────────────────
@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: uuid.UUID,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await session.delete(user)
    await session.commit()


# ── Account bindings ─────────────────────────────────────────────────
@router.get("/{user_id}/accounts", response_model=list[AccountBindResponse])
async def list_user_accounts(
    user_id: uuid.UUID,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    result = await session.execute(
        select(UserAccount).where(UserAccount.user_id == user_id).order_by(UserAccount.added_at)
    )
    return result.scalars().all()


@router.post("/{user_id}/accounts", response_model=AccountBindResponse, status_code=status.HTTP_201_CREATED)
async def bind_account(
    user_id: uuid.UUID,
    body: AccountBind,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    # check user exists
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # check duplicate
    existing = await session.execute(
        select(UserAccount).where(
            UserAccount.user_id == user_id,
            UserAccount.account_id == body.account_id,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Account already bound")

    binding = UserAccount(user_id=user_id, account_id=body.account_id)
    session.add(binding)
    await session.commit()
    await session.refresh(binding)
    return binding


@router.delete("/{user_id}/accounts/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unbind_account(
    user_id: uuid.UUID,
    account_id: int,
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
):
    result = await session.execute(
        select(UserAccount).where(
            UserAccount.user_id == user_id,
            UserAccount.account_id == account_id,
        )
    )
    binding = result.scalar_one_or_none()
    if not binding:
        raise HTTPException(status_code=404, detail="Binding not found")
    await session.delete(binding)
    await session.commit()
