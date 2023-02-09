import asyncio
import datetime
import time
from typing import Tuple, List, Optional
from abc import ABC
import logging
import concurrent.futures
import functools

import aioredis

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class StatsAPI(ABC):
    """
    Interface for communication with time-series databases
    """

    async def gauge(self, key: str, lights_status_change_time: datetime.datetime, value: int):
        pass

    async def incr(self, key: str, value: int):
        pass


class QueueAPI(ABC):
    """
    Interface for communication with queues.
    """

    async def get_message(self, queue_name: str) -> Tuple:
        pass

    async def get_light_statuses(self, queue_name: str) -> List[Tuple]:
        pass


class GraphiteStatsAPI(StatsAPI):
    GRAPHITE_HOST = "localhost"
    GRAPHITE_PORT = 2003

    def __init__(self):
        self.writer: Optional[asyncio.StreamWriter] = None
        self.reader: Optional[asyncio.StreamReader] = None

    async def init_connection(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.GRAPHITE_HOST, self.GRAPHITE_PORT
        )

    async def close_connection(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def gauge(self, key: str, lights_status_change_time: int, value: int):
        message = f"{key} {value} {lights_status_change_time}\n"

        await self.init_connection()
        self.writer.write(message.encode())
        await self.writer.drain()
        await self.close_connection()
        return message

    async def bulk_gauge(self, data):
        gauge_tasks = []
        for time_of_change, status in data:
            gauge_tasks.append(
                asyncio.create_task(self.gauge('local.home.async', int(time_of_change.timestamp()), int(status)))
            )
        messages = await asyncio.gather(*gauge_tasks)
        return messages


class RedisQueueAPI(QueueAPI):
    CONNECTION_URL = "redis://localhost"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # most of magic methods cannot be asynchronous.
    def __init__(self):
        log.info('Connecting to redis')
        self.client = aioredis.from_url(self.CONNECTION_URL, decode_responses=True)

    async def get_light_statuses(self, queue_name: str) -> List[Tuple]:
        log.info('Getting all records from queue')
        light_statuses = await self.client.hgetall(queue_name)  # Non blocking code
        await self.client.flushall(asynchronous=True)
        formatted_light_statuses = [
            self._reformat_message(
                lights_status_change_time,
                status
            ) for lights_status_change_time, status in light_statuses.items()
        ]
        return formatted_light_statuses

    @classmethod
    def _reformat_message(cls, lights_status_change_time: str, status: str) -> Tuple:
        try:
            lights_status_change_time = datetime.datetime.strptime(lights_status_change_time, cls.DATETIME_FORMAT)
            status = bool(int(status))
        except ValueError:
            log.error(
                f"Can not cast these values - time: {lights_status_change_time}"
                f" of type {type(lights_status_change_time)}, status: {status} of type {type(status)}", exc_info=True
            )
        return lights_status_change_time, status

    # Blocking code
    async def _reformat_messages(self, light_statuses: dict) -> List[Tuple]:
        formatted_light_statuses = []
        for lights_status_change_time, status in light_statuses:
            try:
                lights_status_change_time = datetime.datetime.strptime(lights_status_change_time, self.DATETIME_FORMAT)
                status = bool(int(status))
            except ValueError:
                log.error(
                    f"Can not cast these values - time: {lights_status_change_time}"
                    f" of type {type(lights_status_change_time)}, status: {status} of type {type(status)}"
                )
            formatted_light_statuses.append((lights_status_change_time, status))
        return formatted_light_statuses


async def main():
    queue = RedisQueueAPI()
    stats = GraphiteStatsAPI()
    light_status_events = await queue.get_light_statuses('map_async_home')
    await stats.bulk_gauge(light_status_events)

if __name__ == "__main__":
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())

    # =================
    start = time.time()
    asyncio.run(main())
    print(f'Time of execution: {time.time() - start}')
