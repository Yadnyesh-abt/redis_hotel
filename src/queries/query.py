from sqlalchemy.ext.asyncio import AsyncSession
from models.hotel import Hotel, Country, State, City, Area
from interfaces.pydantic import HotelCreate, HotelUpdate
from typing import Optional
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload


# <-----------Insert Query----------------------->
async def insert_hotel(db: AsyncSession, hotel: HotelCreate) -> Hotel:
    # Get or create location IDs
    country_id = await get_or_create_location(db, Country, "country", hotel.country)
    state_id = await get_or_create_location(db, State, "state", hotel.state)
    city_id = await get_or_create_location(db, City, "city", hotel.city)
    area_id = await get_or_create_location(db, Area, "area", hotel.area)

    new_hotel = Hotel(
        name=hotel.name,
        description=hotel.description,
        streetaddress=hotel.streetaddress,
        country_id=country_id,
        state_id=state_id,
        city_id=city_id,
        area_id=area_id,
    )

    db.add(new_hotel)
    await db.commit()
    await db.refresh(new_hotel)

    result = await db.execute(
        select(Hotel)
        .where(Hotel.id == new_hotel.id)
        .options(
            joinedload(Hotel.country),
            joinedload(Hotel.state),
            joinedload(Hotel.city),
            joinedload(Hotel.area),
        )
    )
    hotel = result.scalar_one_or_none()

    return {
        "id": hotel.id,
        "name": hotel.name,
        "description": hotel.description,
        "streetaddress": hotel.streetaddress,
        "country": hotel.country.country,  # Fetching the name from the related Country table
        "state": hotel.state.state,  # Fetching the name from the related State table
        "city": hotel.city.city,  # Fetching the name from the related City table
        "area": hotel.area.area,  # Fetching the name from the related Area table
    }


# <------------------Function to check if country,state,city,area already in db, if not create it ------------------->
async def get_or_create_location(db: AsyncSession, model, name_field: str, value: str):
    stmt = select(model).where(getattr(model, name_field) == value)
    result = await db.execute(stmt)
    instance = result.scalar_one_or_none()

    if instance:
        return instance.id

    # Not found, create new entry
    new_instance = model(**{name_field: value})
    db.add(new_instance)
    await db.flush()  # Gets the ID without commit
    return new_instance.id


# <-------------------Query to retrive hotel from filters---------------------------->
async def get_hotel(
    db: AsyncSession,
    country: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    hotel_id: Optional[int] = None,
):
    if hotel_id:
        # If hotel_id is provided, return specific hotel
        result = await db.execute(
            select(Hotel)
            .where(Hotel.id == hotel_id)
            .options(
                joinedload(Hotel.country),
                joinedload(Hotel.state),
                joinedload(Hotel.city),
                joinedload(Hotel.area),
            )
        )
        hotel = result.scalar_one_or_none()
        if hotel:
            # Return the hotel with names for country, state, city, area
            return {
                "id": hotel.id,
                "name": hotel.name,
                "description": hotel.description,
                "streetaddress": hotel.streetaddress,
                "country": hotel.country.country,  # Fetching the name from the related Country table
                "state": hotel.state.state,  # Fetching the name from the related State table
                "city": hotel.city.city,  # Fetching the name from the related City table
                "area": hotel.area.area,  # Fetching the name from the related Area table
            }
        return None

    else:
        # If no hotel_id is provided, apply filters
        query = select(Hotel).options(
            joinedload(Hotel.country),
            joinedload(Hotel.state),
            joinedload(Hotel.city),
            joinedload(Hotel.area),
        )

        if country:
            query = query.where(Hotel.country.has(Country.country == country))
        if state:
            query = query.where(Hotel.state.has(State.state == state))
        if city:
            query = query.where(Hotel.city.has(City.city == city))
        if area:
            query = query.where(Hotel.area.has(Area.area == area))

        result = await db.execute(query)
        all_hotels = result.scalars().all()

        hotels_with_names = []
        for hotel in all_hotels:
            hotels_with_names.append(
                {
                    "id": hotel.id,
                    "name": hotel.name,
                    "description": hotel.description,
                    "streetaddress": hotel.streetaddress,
                    "country": hotel.country.country,  # Fetching the name from the related Country table
                    "state": hotel.state.state,  # Fetching the name from the related State table
                    "city": hotel.city.city,  # Fetching the name from the related City table
                    "area": hotel.area.area,  # Fetching the name from the related Area table
                }
            )

        return hotels_with_names


# <---------------- query to update data ----------------------------------->
async def update_hotels(db: AsyncSession, hotel_data: HotelUpdate) -> Optional[dict]:
    result = await db.execute(select(Hotel).where(Hotel.id == hotel_data.id))
    hotel = result.scalar_one_or_none()

    if not hotel:
        return None

    update_data = hotel_data.model_dump(exclude_unset=True)
    update_data.pop("id", None)  # We don't want to update the ID

    # Mapping for foreign key fields (name -> model -> field name)
    fk_mappings = {
        "country": (Country, "country"),
        "state": (State, "state"),
        "city": (City, "city"),
        "area": (Area, "area"),
    }

    for field, (model, field_name) in fk_mappings.items():
        if field in update_data:
            value = update_data[field]
            if value:
                # Get the existing object if it exists, or create it
                result = await db.execute(
                    select(model).where(getattr(model, field_name) == value)
                )
                obj = result.scalar_one_or_none()

                if not obj:
                    obj = model(**{field_name: value})
                    db.add(obj)
                    await db.flush()  # Get the ID without committing

                update_data[field] = obj.id
            else:
                # If value is None, remove the field from update_data
                update_data.pop(field)

    # Apply remaining non-foreign-key updates
    for field, value in update_data.items():
        setattr(hotel, field, value)

    await db.commit()
    await db.refresh(hotel)

    return hotel.id, update_data


# <-----------------------query to delete hotel --------------->
async def delete_hotel(db: AsyncSession, hotel_id: int) -> bool:
    result = await db.execute(select(Hotel).where(Hotel.id == hotel_id))
    hotel = result.scalar_one_or_none()

    if hotel is None:
        return False  # Hotel not found

    await db.delete(hotel)
    await db.commit()
    return True
