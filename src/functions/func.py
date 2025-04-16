from redis.asyncio import Redis
from queries.query import insert_hotel, get_hotel, update_hotels, delete_hotel
from interfaces.pydantic import Hotel
from typing import Dict, Any, Optional, List



REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None
REDIS_TTL = 60 * 60 * 24

redis = Redis.from_url(
    f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    password=REDIS_PASSWORD,
    decode_responses=True,
)


# <---------Function to insert in DB----------------->
async def add_hotel(hotel, db):
    data = await insert_hotel(db, hotel)  # insert query function call
    await store_location_data(data)  # function call for redis storage
    return data


# <---------Function to update data in DB----------------->
async def update_hotel(hotel, db):
    id, data = await update_hotels(db, hotel)  # update query Function call

    return await update_simple_fields(id, data)  # function call to update data in redis


# <---------Function to get data from DB----------------->
async def get_hotels(
    db,
    country: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    hotel_id: Optional[str] = None,
):
    redis_key = ""
    if country:
        redis_key = country
        data = await retrieve_location_data(
            redis_key
        )  # function to get data from redis for particular country
        if not data:  # if not present in redis
            data = await get_hotel(
                db=db, country=country
            )  # get query function call for particular country
            await store_location_data(data)  # function call to store in redis

    elif state:
        data = await retrieve_with_state(
            state
        )  # function to get data using reverse indexing for state

        if not data:  # if not present in redis
            data = await get_hotel(db=db, state=state)  # get query for particular state
            await store_location_data(data)  # function call to store in redis

    elif city:
        data = await retrieve_with_city(
            city
        )  # function to get data using reverse indexing for city

        if not data:  # if not present in redis
            data = await get_hotel(db=db, city=city)  # get query for particular city
            await store_location_data(data)  # function call to store in redis

    elif area:
        data = await retrieve_with_area(
            area
        )  # function to get data using reverse indexing for city

        if not data:  # if not present in redis
            data = await get_hotel(db=db, area=area)  # get query for particular area
            await store_location_data(data)  # function call to store in redis
    else:
        # if no filter is provided get all data
        data = (
            await retrieve_location_data()
        )  # function call to get all data from redis

        if not data:  # if not present in redis
            data = await get_hotel(db=db)  # get query for all hotels

            await store_location_data(data)  # function call to store in redis

    return data


# <---------Function to Delete data from DB----------------->
async def delete(id, db):
    await delete_hotel(db, id)  # delete query function call
    data = await delete_location_data(id)  # function call to delete data from redis
    return data


# <----------Function to create sets and store hashkeys in sets--------------------->
async def add_to_hierarchical_sets(pipe, hash_key: str, data: Hotel):
    """Add the hash key to hierarchical sets"""
    # Country set
    country_set = data.country
    await pipe.sadd(country_set, hash_key)
    await pipe.expire(country_set, REDIS_TTL)

    # Country:State set
    state_set = f"{data.country}:{data.state}"
    await pipe.sadd(state_set, hash_key)
    await pipe.expire(state_set, REDIS_TTL)

    # Country:State:City set
    city_set = f"{data.country}:{data.state}:{data.city}"
    await pipe.sadd(city_set, hash_key)
    await pipe.expire(city_set, REDIS_TTL)

    # Country:State:City:Area set
    area_set = f"{data.country}:{data.state}:{data.city}:{data.area}"
    await pipe.sadd(area_set, hash_key)
    await pipe.expire(area_set, REDIS_TTL)


# <------------ Function to create reverse index for each filter attribute --------------->
async def create_reverse_indices(pipe, data: Hotel):
    """Create reverse indices for area, city, and state"""
    # Area -> Country:State:City
    area_reverse_key = f"reverse:area:{data.area}"
    await pipe.set(area_reverse_key, f"{data.country}:{data.state}:{data.city}")
    await pipe.expire(area_reverse_key, REDIS_TTL)

    # City -> Country:State
    city_reverse_key = f"reverse:city:{data.city}"
    await pipe.set(city_reverse_key, f"{data.country}:{data.state}")
    await pipe.expire(city_reverse_key, REDIS_TTL)

    # State -> Country
    state_reverse_key = f"reverse:state:{data.state}"
    await pipe.set(state_reverse_key, data.country)
    await pipe.expire(state_reverse_key, REDIS_TTL)


# <------------------Function to store data in redis----------------->
async def store_location_data(data_list: List[Hotel]):
    """
    Store location data in Redis with appropriate indexing

    Args:
        data_list: List of Hotel objects to store
    """

    all_keys_set = "ALL"

    # Use Redis pipeline for batched operations
    async with redis.pipeline(transaction=False) as pipe:
        for d in data_list:
            # Use the ID from the data as the hash key identifier
            data = Hotel(**d)
            hash_key = f"loc:{data.id}"

            # Store the location data as a hash
            await pipe.hset(hash_key, mapping=data.dict())
            await pipe.expire(hash_key, REDIS_TTL)

            # Add to the ALL set
            await pipe.sadd(all_keys_set, hash_key)
            await pipe.expire(all_keys_set, REDIS_TTL)

            # Add to country set
            country_set = data.country
            await pipe.sadd(country_set, hash_key)
            await pipe.expire(country_set, REDIS_TTL)

            # Add to country:state set
            state_set = f"{data.country}:{data.state}"
            await pipe.sadd(state_set, hash_key)
            await pipe.expire(state_set, REDIS_TTL)

            # Add to country:state:city set
            city_set = f"{data.country}:{data.state}:{data.city}"
            await pipe.sadd(city_set, hash_key)
            await pipe.expire(city_set, REDIS_TTL)

            # Add to country:state:city:area set
            area_set = f"{data.country}:{data.state}:{data.city}:{data.area}"
            await pipe.sadd(area_set, hash_key)
            await pipe.expire(area_set, REDIS_TTL)

            # Create reverse indices
            # Area -> Country:State:City
            area_reverse_key = f"reverse:area:{data.area}"
            await pipe.set(area_reverse_key, f"{data.country}:{data.state}:{data.city}")
            await pipe.expire(area_reverse_key, REDIS_TTL)

            # City -> Country:State
            city_reverse_key = f"reverse:city:{data.city}"
            await pipe.set(city_reverse_key, f"{data.country}:{data.state}")
            await pipe.expire(city_reverse_key, REDIS_TTL)

            # State -> Country
            state_reverse_key = f"reverse:state:{data.state}"
            await pipe.set(state_reverse_key, data.country)
            await pipe.expire(state_reverse_key, REDIS_TTL)

        # Execute all Redis commands in the pipeline
        await pipe.execute()

    await redis.close()


# <-------------------Function for retriving structure of set  from reverse index of area ----------------------->
async def reverse_lookup_area(area: str) -> Optional[str]:
    """
    Get Country:State:City for a given area

    Args:
        area: The area to look up

    Returns:
        A string in the format "Country:State:City" or None if not found
    """

    result = await redis.get(f"reverse:area:{area}")
    await redis.close()
    return result


# <-------------------Function for retriving structure of set from reverse index of city ----------------------->
async def reverse_lookup_city(city: str) -> Optional[str]:
    """
    Get Country:State for a given city

    Args:
        city: The city to look up

    Returns:
        A string in the format "Country:State" or None if not found
    """

    result = await redis.get(f"reverse:city:{city}")
    await redis.close()
    return result


# <-------------------Function for retriving structure of set from reverse index of state ----------------------->
async def reverse_lookup_state(state: str) -> Optional[str]:
    """
    Get Country for a given state

    Args:
        state: The state to look up

    Returns:
        The country name or None if not found
    """

    result = await redis.get(f"reverse:state:{state}")
    await redis.close()
    return result


# <-------------------Function for get particular data using redis key and to get all data without using any input ----------------------->
async def retrieve_location_data(key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve location data based on the provided key

    Args:
        key: The key to retrieve data for (e.g., "India", "India:Maharashtra", etc.)
            If None, all data will be retrieved

    Returns:
        A list of location data dictionaries
    """

    if key is None:
        key = "ALL"

    # Get all hash keys from the set
    hash_keys = await redis.smembers(key)
    if not hash_keys:
        await redis.close()
        return []

    result = []

    # Use pipeline for batched operations
    async with redis.pipeline(transaction=False) as pipe:
        # Queue up all the hgetall operations
        for hash_key in hash_keys:
            pipe.hgetall(hash_key)

        # Execute and get all hash data
        hash_data_list = await pipe.execute()

        # Process the results
        for hash_data in hash_data_list:
            if hash_data:  # Skip empty results
                result.append(hash_data)

    await redis.close()
    return result


async def retrieve_with_area(area: str) -> List[Dict[str, Any]]:
    """
    Retrieve location data for a given area using reverse indexing

    Args:
        area: The area to retrieve data for

    Returns:
        A list of location data dictionaries
    """
    location_key = await reverse_lookup_area(area)
    print("key:", location_key)
    if not location_key:
        return []

    # Now use the location key to get the data
    return await retrieve_location_data(f"{location_key}:{area}")


async def retrieve_with_city(city: str) -> List[Dict[str, Any]]:
    """
    Retrieve location data for a given city using reverse indexing

    Args:
        city: The city to retrieve data for

    Returns:
        A list of location data dictionaries
    """
    location_key = await reverse_lookup_city(city)
    if not location_key:
        return []

    # Now use the location key to get the data
    return await retrieve_location_data(f"{location_key}:{city}")


async def retrieve_with_state(state: str) -> List[Dict[str, Any]]:
    """
    Retrieve location data for a given state using reverse indexing

    Args:
        state: The state to retrieve data for

    Returns:
        A list of location data dictionaries
    """
    country = await reverse_lookup_state(state)
    if not country:
        return []

    # Now use the country and state to get the data
    return await retrieve_location_data(f"{country}:{state}")


async def update_simple_fields(
    location_id: int, update_data: Dict[str, str]
) -> Dict[str, Any]:
    """
    Update only the simple fields (name, description, streetaddress) for a location

    Args:
        location_id: The unique ID of the location to update
        update_data: Dictionary containing only the fields to update (name, description, streetaddress)

    Returns:
        The updated location data after changes

    Raises:
        ValueError: If the location ID doesn't exist in Redis or invalid fields are provided
    """
    # Validate input fields - only allow updates to these specific fields
    allowed_fields = {"name", "description", "streetaddress"}

    # Check if update_data contains only allowed fields
    if not all(field in allowed_fields for field in update_data.keys()):
        invalid_fields = [
            field for field in update_data.keys() if field not in allowed_fields
        ]
        raise ValueError(
            f"Cannot update fields: {invalid_fields}. Only name, description, and streetaddress can be updated."
        )

    # If dictionary is empty, return early
    if not update_data:
        raise ValueError("No valid fields provided for update")

    hash_key = f"loc:{location_id}"

    # Check if the hash key exists
    exists = await redis.exists(hash_key)
    if not exists:
        await redis.close()
        raise ValueError(f"Location with ID {location_id} does not exist in Redis")

    try:
        # Update the hash with new values (only the provided fields)
        await redis.hset(hash_key, mapping=update_data)
        # Reset TTL to keep the data fresh
        await redis.expire(hash_key, REDIS_TTL)

        # Retrieve and return the updated data
        updated_data = await redis.hgetall(hash_key)
        return updated_data

    except Exception as e:
        raise Exception(f"Error updating location data: {str(e)}")
    finally:
        await redis.close()


async def delete_location_data(location_id: int) -> Dict[str, Any]:
    """
    Delete a location from Redis by its ID, removing it from all related sets

    Args:
        location_id: The unique ID of the location to delete

    Returns:
        A dictionary with deletion status and information about what was deleted

    Raises:
        ValueError: If the location ID doesn't exist in Redis
    """

    hash_key = f"loc:{location_id}"

    # Check if the hash key exists
    exists = await redis.exists(hash_key)
    if not exists:
        await redis.close()
        raise ValueError(f"Location with ID {location_id} does not exist in Redis")

    try:
        # First, get the location data to find the sets it belongs to
        location_data = await redis.hgetall(hash_key)

        # Extract hierarchical values
        country = location_data.get("country")
        state = location_data.get("state")
        city = location_data.get("city")
        area = location_data.get("area")

        # Create a list of sets to remove the hash_key from
        sets_to_update = [
            "ALL",  # Global set containing all locations
            country,  # Country set
            f"{country}:{state}",  # Country:State set
            f"{country}:{state}:{city}",  # Country:State:City set
            f"{country}:{state}:{city}:{area}",  # Country:State:City:Area set
        ]

        # Use pipeline for batched operations
        async with redis.pipeline(transaction=False) as pipe:
            # Remove the hash_key from all sets
            for set_key in sets_to_update:
                await pipe.srem(set_key, hash_key)

            # Delete the hash itself
            await pipe.delete(hash_key)

            # Execute all operations
            await pipe.execute()

        return {
            "status": "success",
            "message": f"Location with ID {location_id} deleted",
            "deleted_data": location_data,
            "removed_from_sets": sets_to_update,
        }

    except Exception as e:
        raise Exception(f"Error deleting location data: {str(e)}")
    finally:
        await redis.close()
