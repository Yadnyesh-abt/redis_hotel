from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()


username = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")

# SQLAlchemy connection URL
DATABASE_URL = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"


engine = create_async_engine(
    DATABASE_URL,
    pool_size=20,  # Max connections in the pool
    max_overflow=10,  # Extra connections during peak load
    pool_timeout=30,  # Timeout for acquiring a connection
    pool_pre_ping=True,  # Ensure connections are alive
    echo=False,  # Enable SQL logging for debugging
)


# Create async session factory
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,  # Prevent objects from expiring after commit
    autoflush=False,  # Manual flushing for control
)


# Dependency to provide async session
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db
