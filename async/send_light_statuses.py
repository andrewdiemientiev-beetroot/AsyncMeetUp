import asyncio
import calendar
from datetime import datetime

import aioredis


def generate_mock_data():
    lights_status = True
    lights_statuses = dict()
    year = 2019
    for month in range(1, 13):
        _, days = calendar.monthrange(year, month)
        for day in range(1, days):
            for hour in range(0, 24):
                lights_status_change_datetime = str(datetime(year, month, day, hour, 0, 0))
                lights_statuses[lights_status_change_datetime] = int(lights_status)
                lights_status = not lights_status
    return lights_statuses


async def main():
    lights_statuses = generate_mock_data()
    print(len(lights_statuses))
    redis = aioredis.from_url("redis://localhost", decode_responses=True)
    await redis.hset('map_async_home', mapping=lights_statuses)
    await redis.close()


asyncio.run(main())
