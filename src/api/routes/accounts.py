"""
Admin API — manage trading accounts (CRUD) + credential verification.

These endpoints allow creating, listing, updating, and deleting
the MT5 trading accounts that the trader process connects to.
The poller's system account is configured via .env and is NOT
managed here.

Credential verification is performed via Redis RPC: the API publishes
a verify request and the trader process (running natively on Windows
with MT5 access) executes the actual login attempt and responds.
"""

from __future__ import annotations

import uuid

import orjson
from fastapi import APIRouter, HTTPException, status

from src.api.schemas import (
    AccountCreate,
    AccountResponse,
    AccountUpdate,
    VerifyRequest,
    VerifyResponse,
)
from src.config import get_settings
from src.db import trading_repository as repo
from src.redis_bus.pool import get_redis_pool

router = APIRouter(prefix="/api/v1/admin/accounts", tags=["admin"])


# ---------------------------------------------------------------
# Notify trader of account changes (hot-reload)
# ---------------------------------------------------------------

async def _notify_trader_reload() -> None:
    """Publish a reload signal so the trader picks up account changes immediately."""
    try:
        r = get_redis_pool()
        await r.publish("trader:account:reload", "reload")
    except Exception:
        pass  # best-effort; trader also polls every 60 s


# ---------------------------------------------------------------
# Verify helper (Redis RPC to trader process)
# ---------------------------------------------------------------

_VERIFY_TIMEOUT_SEC = 60


async def _verify_via_trader(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
    mt5_path: str | None = None,
) -> dict:
    """Send a verify request to the trader process via Redis and wait for the response."""
    settings = get_settings()
    path = mt5_path or settings.mt5_path

    req_id = uuid.uuid4().hex
    req_key = f"trader:verify:{req_id}:req"
    resp_key = f"trader:verify:{req_id}:resp"

    payload = orjson.dumps({
        "mt5_login": mt5_login,
        "mt5_password": mt5_password,
        "mt5_server": mt5_server,
        "mt5_path": path,
    })

    r = get_redis_pool()
    await r.set(req_key, payload, ex=_VERIFY_TIMEOUT_SEC + 10)

    # Notify the trader via pub/sub so it picks up the request immediately
    await r.publish("trader:verify:requests", req_id)

    # Wait for the response
    import asyncio
    for _ in range(_VERIFY_TIMEOUT_SEC * 10):  # poll every 100ms
        raw = await r.get(resp_key)
        if raw is not None:
            await r.delete(req_key, resp_key)
            return orjson.loads(raw)
        await asyncio.sleep(0.1)

    # Cleanup on timeout
    await r.delete(req_key)
    raise HTTPException(
        status.HTTP_504_GATEWAY_TIMEOUT,
        "Trader process did not respond. Is it running?",
    )


# ---------------------------------------------------------------
# Verify endpoint
# ---------------------------------------------------------------

@router.post(
    "/verify",
    response_model=VerifyResponse,
    summary="Verify MT5 credentials",
    description=(
        "Perform a real MT5 login attempt without creating any "
        "account record. Returns account info on success or a "
        "clear error on failure.\n\n"
        "Requires the trader process to be running."
    ),
)
async def verify_account(body: VerifyRequest):
    result = await _verify_via_trader(
        mt5_login=body.mt5_login,
        mt5_password=body.mt5_password,
        mt5_server=body.mt5_server,
        mt5_path=body.mt5_path,
    )

    if result.get("ok"):
        return VerifyResponse(
            ok=True,
            account_name=result.get("account_name", ""),
            server=result.get("server", ""),
            balance=result.get("balance", 0),
            leverage=result.get("leverage", 0),
            currency=result.get("currency", ""),
            message="MT5 login successful",
        )

    # Map MT5 error codes to user-friendly messages
    err_code = result.get("error_code", 0)
    err_msg = result.get("error_msg", "Unknown error")
    detail = _mt5_error_to_detail(err_code, err_msg)
    raise HTTPException(status.HTTP_400_BAD_REQUEST, detail)


def _mt5_error_to_detail(err_code: int, err_msg: str) -> str:
    """Convert MT5 error code to a human-readable message."""
    mapping = {
        -6: "Invalid MT5 credentials",
        -3: "MT5 terminal path is invalid",
        -7: "Unknown MT5 server",
        -10003: "MT5 initialize failed",
        -10004: "No IPC connection — MT5 terminal not available",
    }
    return mapping.get(err_code, f"MT5 error {err_code}: {err_msg}")


# ---------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------

@router.get("", response_model=list[AccountResponse])
async def list_accounts():
    """List all registered trading accounts (passwords are masked)."""
    return await repo.list_accounts()


@router.get("/{account_id}", response_model=AccountResponse)
async def get_account(account_id: int):
    """Get a single trading account by ID."""
    row = await repo.get_account(account_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")
    row.pop("mt5_password", None)
    return row


@router.post(
    "",
    response_model=AccountResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_account(body: AccountCreate):
    """Register a new trading account.

    If ``verify_credentials`` is true, performs a real MT5 login before
    saving.  Returns 400 if the credentials are invalid.
    """
    if body.verify_credentials:
        result = await _verify_via_trader(
            mt5_login=body.mt5_login,
            mt5_password=body.mt5_password,
            mt5_server=body.mt5_server,
        )
        if not result.get("ok"):
            err_code = result.get("error_code", 0)
            err_msg = result.get("error_msg", "Unknown error")
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                _mt5_error_to_detail(err_code, err_msg),
            )

    try:
        settings = get_settings()
        # Always use a per-login portable terminal path.
        portable_path = (
            f"{settings.mt5_portable_dir}\\{body.mt5_login}\\terminal64.exe"
        )
        row = await repo.create_account(
            label=body.label,
            mt5_login=body.mt5_login,
            mt5_password=body.mt5_password,
            mt5_server=body.mt5_server,
            mt5_path=portable_path,
            enabled=body.enabled,
            description=body.description,
        )
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "Account with this login or label already exists",
            )
        raise
    await _notify_trader_reload()
    return row


@router.patch("/{account_id}", response_model=AccountResponse)
async def update_account(account_id: int, body: AccountUpdate):
    """Update fields of an existing trading account.

    If ``verify_credentials`` is true, the API verifies the merged
    credentials (existing + patch) before saving.  When changing
    ``mt5_login`` or ``mt5_server``, ``mt5_password`` must be provided.

    Uniqueness of ``label`` and ``mt5_login`` is checked excluding the
    current account, so a password-only update never triggers a false
    duplicate error.
    """
    fields = body.model_dump(exclude_unset=True)
    fields.pop("verify_credentials", None)

    if not fields:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "No fields to update",
        )

    # When mt5_login changes, auto-update the portable terminal path
    if "mt5_login" in fields and "mt5_path" not in fields:
        settings = get_settings()
        fields["mt5_path"] = (
            f"{settings.mt5_portable_dir}\\{fields['mt5_login']}\\terminal64.exe"
        )

    if body.verify_credentials:
        changing_login = "mt5_login" in fields
        changing_server = "mt5_server" in fields

        if (changing_login or changing_server) and "mt5_password" not in fields:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "mt5_password is required when changing mt5_login or mt5_server "
                "with verify_credentials=true",
            )

        # Need full record to merge unchanged fields
        existing = await repo.get_account(account_id)
        if existing is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")

        verify_login = fields.get("mt5_login", existing["mt5_login"])
        verify_password = fields.get("mt5_password", existing["mt5_password"])
        verify_server = fields.get("mt5_server", existing["mt5_server"])
        verify_path = fields.get("mt5_path", existing.get("mt5_path"))

        result = await _verify_via_trader(
            mt5_login=verify_login,
            mt5_password=verify_password,
            mt5_server=verify_server,
            mt5_path=verify_path,
        )
        if not result.get("ok"):
            err_code = result.get("error_code", 0)
            err_msg = result.get("error_msg", "Unknown error")
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                _mt5_error_to_detail(err_code, err_msg),
            )

    try:
        row = await repo.update_account(account_id, **fields)
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "Account with this login or label already exists",
            )
        raise
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")
    await _notify_trader_reload()
    return row


@router.post(
    "/{account_id}/verify-update",
    response_model=VerifyResponse,
    summary="Verify merged credentials for an existing account",
    description=(
        "Merge the current account record with the provided partial "
        "update, then perform a real MT5 login attempt. Nothing is "
        "saved. Useful for validating a password change before "
        "committing it."
    ),
)
async def verify_update(account_id: int, body: AccountUpdate):
    existing = await repo.get_account(account_id)
    if existing is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")

    patch = body.model_dump(exclude_unset=True)
    patch.pop("verify_credentials", None)

    merged_login = patch.get("mt5_login", existing["mt5_login"])
    merged_password = patch.get("mt5_password", existing["mt5_password"])
    merged_server = patch.get("mt5_server", existing["mt5_server"])
    merged_path = patch.get("mt5_path", existing.get("mt5_path"))

    result = await _verify_via_trader(
        mt5_login=merged_login,
        mt5_password=merged_password,
        mt5_server=merged_server,
        mt5_path=merged_path,
    )

    if result.get("ok"):
        return VerifyResponse(
            ok=True,
            account_name=result.get("account_name", ""),
            server=result.get("server", ""),
            balance=result.get("balance", 0),
            leverage=result.get("leverage", 0),
            currency=result.get("currency", ""),
            message="MT5 login successful",
        )

    err_code = result.get("error_code", 0)
    err_msg = result.get("error_msg", "Unknown error")
    raise HTTPException(status.HTTP_400_BAD_REQUEST, _mt5_error_to_detail(err_code, err_msg))


@router.delete("/{account_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_account(account_id: int):
    """Delete a trading account.

    Deals and positions already synced to the database are NOT deleted.
    """
    ok = await repo.delete_account(account_id)
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")
    await _notify_trader_reload()


# ---------------------------------------------------------------
# Force re-sync deal history
# ---------------------------------------------------------------

@router.post(
    "/{account_id}/sync-history",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Force full deal-history resync",
    description=(
        "Set a flag in Redis that tells the trader process to pull "
        "the **complete** deal history from MT5 for this account on "
        "the next sync cycle (~60 s).  The trader uses "
        "`history_deals_get` from 2000-01-01 to now and upserts all "
        "deals.  Returns 202 immediately — the sync happens "
        "asynchronously in the trader process."
    ),
)
async def sync_history(account_id: int):
    """Request a full deal-history resync for an account."""
    row = await repo.get_account(account_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")

    r = get_redis_pool()
    await r.set(f"trader:resync:{account_id}", "1", ex=300)
    return {"status": "accepted", "account_id": account_id, "message": "Full deal history resync requested. Will complete within ~60 seconds."}
