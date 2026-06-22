#!/bin/sh
set -e

echo "Running DB migrations..."
python -c "
import asyncio, os
from sqlalchemy.ext.asyncio import create_async_engine
from app.db.models import Base

async def init():
    url = os.environ['MONITOR_DATABASE_URL']
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print('Tables ready.')

asyncio.run(init())
"

echo "Seeding admin user..."
python -m app.seed_admin

echo "Starting monitor-api..."
exec uvicorn app.main:create_app --host 0.0.0.0 --port 8080 --factory --workers 1
