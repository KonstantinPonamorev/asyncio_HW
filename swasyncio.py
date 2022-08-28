import asyncio
import aiohttp
from more_itertools import chunked
import asyncpg
import database


MAX_CHUNK = 20


async def get_people(session, people_id):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        return json_data


async def get_film_title(session, film_url):
    async with session.get(film_url) as response:
        json_data = await response.json()
        return json_data["title"]


async def get_object_name(session, object_url):
    async with session.get(object_url) as response:
        json_data = await response.json()
        return json_data["name"]


async def get_all_data():
    async with aiohttp.ClientSession() as session:
        coroutines = (get_people(session, i) for i in range(1, 83))
        result = []
        for coroutines_chunk in chunked(coroutines, MAX_CHUNK):
            peoples = await asyncio.gather(*coroutines_chunk)
            for item in peoples:
                result.append(item)
        for item in result:
            coroutines_films = []
            for film in item.get('films', []):
                coroutines_films.append(get_film_title(session, film))
            coroutines_species = []
            for species in item.get('species', []):
                coroutines_species.append(get_object_name(session, species))
            coroutines_starships = []
            for starships in item.get('starships', []):
                coroutines_starships.append(get_object_name(session, starships))
            coroutines_vehicles = []
            for vehicles in item.get('vehicles', []):
                coroutines_vehicles.append(get_object_name(session, vehicles))
            item['films'] = await asyncio.gather(*coroutines_films)
            item['films'] = ', '.join(item['films'])
            item['species'] = await asyncio.gather(*coroutines_species)
            item['species'] = ', '.join(item['species'])
            item['starships'] = await asyncio.gather(*coroutines_starships)
            item['starships'] = ', '.join(item['starships'])
            item['vehicles'] = await asyncio.gather(*coroutines_vehicles)
            item['vehicles'] = ', '.join(item['vehicles'])
        return result


async def insert_data(pool: asyncpg.Pool, data):
    query = 'INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, ' \
            'homeworld, mass, name, skin_color, species, starships, vehicles) VALUES ($1, $2, $3, $4, ' \
            '$5, $6, $7, $8, $9, $10, $11, $12, $13)'
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, data)



async def main():
    new_db = database.NewDataBase(database.DB)
    data = await get_all_data()
    pool = await asyncpg.create_pool(database.DB, min_size=20, max_size=20)
    tasks = []
    for people_chunk in chunked(data, 100):
        tasks.append(asyncio.create_task(insert_data(pool, people_chunk)))
    await asyncio.gather(*tasks)
    await pool.close()


asyncio.run(main())
print('ok')



