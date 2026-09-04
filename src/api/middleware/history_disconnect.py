"""Cancel read-only history handlers when their HTTP client disconnects."""
from __future__ import annotations

import asyncio
from contextlib import suppress


class HistoryDisconnectMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        paths = ("/api/v1/ticks/", "/api/v1/candles/custom/", "/api/v1/spread/")
        if scope["type"] != "http" or scope.get("method") != "GET" or not scope["path"].startswith(paths):
            return await self.app(scope, receive, send)

        messages = asyncio.Queue()
        disconnected = asyncio.Event()

        async def listen():
            while True:
                message = await receive()
                await messages.put(message)
                if message["type"] == "http.disconnect":
                    disconnected.set()
                    return

        listener = asyncio.create_task(listen())
        handler = asyncio.create_task(self.app(scope, messages.get, send))
        signal = asyncio.create_task(disconnected.wait())
        try:
            done, _ = await asyncio.wait((handler, signal), return_when=asyncio.FIRST_COMPLETED)
            if handler in done:
                await handler
            else:
                handler.cancel()
                with suppress(asyncio.CancelledError):
                    await handler
        finally:
            for task in (listener, handler, signal):
                if not task.done():
                    task.cancel()
            await asyncio.gather(listener, handler, signal, return_exceptions=True)
