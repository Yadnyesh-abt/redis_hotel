from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from configs.connect import get_db
from interfaces.pydantic import HotelCreate, HotelUpdate
from functions.func import add_hotel, get_hotels, update_hotel, delete
from typing import Optional

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Hello, world!"}


@router.post("/hotels")
async def create_hotel(hotel: HotelCreate, db: AsyncSession = Depends(get_db)):
    return await add_hotel(hotel, db)


@router.put("/hotels")
async def change_hotel(hotel: HotelUpdate, db: AsyncSession = Depends(get_db)):
    return await update_hotel(hotel, db)


@router.get("/hotels")
async def fetch_hotels(
    country: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    hotel_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
):
    return await get_hotels(
        db, country=country, state=state, city=city, area=area, hotel_id=hotel_id
    )


@router.delete("/hotel")
async def remove_hotels(id: int, db: AsyncSession = Depends(get_db)):
    return await delete(id, db)
