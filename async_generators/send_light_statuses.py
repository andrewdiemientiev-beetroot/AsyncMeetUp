import asyncio
import calendar
import random
from datetime import datetime

import aioredis


async def generate_mock_data():
    lights_status = True
    year = 2017
    for month in range(1, 13):
        _, days = calendar.monthrange(year, month)
        for day in range(1, days):
            for hour in range(0, 24):
                minutes = random.randint(0, 59)
                lights_status_change_datetime = str(datetime(year, month, day, hour, minutes, 0))
                yield f"{lights_status_change_datetime},{int(lights_status)}"
                lights_status = not lights_status


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)
    async for lights_status in generate_mock_data():
        await redis.lpush('list_home', lights_status)
    await redis.close()


asyncio.run(main())
