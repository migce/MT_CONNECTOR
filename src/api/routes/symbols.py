"""
REST endpoint: ``/api/v1/symbols``

List configured symbols and their basic metadata.
"""

from __future__ import annotations

from fastapi import APIRouter

from src.api.schemas import SymbolInfo
from src.config import get_settings

router = APIRouter(prefix="/api/v1", tags=["symbols"])


@router.get(
    "/symbols",
    response_model=list[SymbolInfo],
    summary="List tracked symbols",
    description="Return the list of symbols currently tracked by the connector.",
)
async def get_symbols() -> list[SymbolInfo]:
    settings = get_settings()
    return [
        SymbolInfo(symbol=s) for s in settings.symbols
    ]
