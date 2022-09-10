import asyncio
import aiohttp
from more_itertools import chunked
import asyncpg
import database

import config


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
        coroutines = (get_people(session, i) for i in range(1, 84))
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
            if 'created' in item:
                item.pop('created')
            if 'edited' in item:
                item.pop('edited')
            if 'url' in item:
                item.pop('url')
            if 'detail' in item:
                result.remove(item)
        result.pop(16)
        return result


def transform_data(result):
    new_data = []
    for item in result:
        new_data.append(tuple(item.values()))
    return new_data


async def main():
    await database.get_async_session(True, True)
    data = await get_all_data()
    new_data = transform_data(data)
    pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)
    tasks = []
    for people_chunk in chunked(new_data, MAX_CHUNK):
        tasks.append(asyncio.create_task(database.insert_people(pool, people_chunk)))
    await asyncio.gather(*tasks)
    await pool.close()


if __name__ == '__main__':
    asyncio.run(main())




