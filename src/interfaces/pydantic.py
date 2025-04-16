from pydantic import BaseModel
from typing import Optional


class HotelCreate(BaseModel):
    name: str
    description: str
    streetaddress: str
    country: str
    state: str
    city: str
    area: str


class Hotel(BaseModel):
    id: int
    name: str
    description: str
    streetaddress: str
    country: str
    state: str
    city: str
    area: str

    class Config:
        orm_mode = True


class HotelUpdate(BaseModel):
    id: int
    name: Optional[str] = None
    description: Optional[str] = None
    streetaddress: Optional[str] = None
