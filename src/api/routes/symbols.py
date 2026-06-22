"""
REST endpoint: ``/api/v1/symbols``

List all symbols available from the MT5 broker.
"""

from __future__ import annotations

from fastapi import APIRouter

from src.api.schemas import SymbolInfo
from src.api.symbol_registry import get_all_mt5_symbols
from src.config import get_settings

router = APIRouter(prefix="/api/v1", tags=["symbols"])


@router.get(
    "/symbols",
    response_model=list[SymbolInfo],
    summary="List available symbols",
    description=(
        "Return all symbols available on the MT5 broker. "
        "Symbols with `tracked=true` are actively polled for real-time data. "
        "Other symbols are available on-demand via the backfill mechanism."
    ),
)
async def get_symbols() -> list[SymbolInfo]:
    settings = get_settings()
    tracked = set(settings.symbols)
    mt5_syms = get_all_mt5_symbols()

    if not mt5_syms:
        # Fallback: registry not loaded yet → return configured symbols only
        return [
            SymbolInfo(symbol=s, tracked=True) for s in settings.symbols
        ]

    result = []
    for name, description in sorted(mt5_syms.items()):
        result.append(SymbolInfo(
            symbol=name,
            description=description,
            tracked=name in tracked,
        ))
    return result
