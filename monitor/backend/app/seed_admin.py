"""
Seed initial admin user.
Run: python -m app.seed_admin
"""
import asyncio
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.auth import hash_password
from app.config import get_settings
from app.db.models import Base, User


async def seed():
    settings = get_settings()
    engine = create_async_engine(settings.database_url)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        result = await session.execute(select(User).where(User.role == "admin"))
        if result.scalar_one_or_none():
            print("Admin user already exists, skipping seed.")
            await engine.dispose()
            return

        admin = User(
            username="admin",
            email="admin@monitor.local",
            password_hash=hash_password("admin"),
            role="admin",
        )
        session.add(admin)
        await session.commit()
        print(f"Admin user created: username=admin password=admin")
        print(">>> CHANGE THE PASSWORD IMMEDIATELY! <<<")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
