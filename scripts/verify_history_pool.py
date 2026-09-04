"""Read-only candidate-image preflight (uses deployed environment, never prints it)."""
import asyncio
from sqlalchemy import text
from src.db.heavy_reads import heavy_read_session, dispose_heavy_engine

async def main():
    try:
        async with heavy_read_session() as session:
            for name in ('statement_timeout','lock_timeout','temp_file_limit','application_name'):
                print(name, await session.scalar(text('SHOW '+name)))
            print('read_probe', await session.scalar(text('SELECT 1')))
    finally:
        await dispose_heavy_engine()

asyncio.run(main())
