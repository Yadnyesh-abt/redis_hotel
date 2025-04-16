from fastapi import FastAPI
from routes.root import router as root_router
from models.hotel import Base
from configs.connect import engine
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all
        )  # Use run_sync to execute synchronously


@app.on_event("startup")
async def startup_event():
    await init_db()


# Include the routers defined in your route files
app.include_router(root_router)
