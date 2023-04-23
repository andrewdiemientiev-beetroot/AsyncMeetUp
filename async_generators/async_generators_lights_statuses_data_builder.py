import asyncio
import datetime
import time
from typing import Tuple, Optional
from abc import ABC
import logging

import aioredis

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

REDIS_QUEUE_NAME = 'list_home'
GRAPHITE_METRIC_NAME = 'local.home.async_gen'


class StatsAPI(ABC):
    """
    Interface for communication with time-series databases
    """

    async def gauge(self, key: str, event_time: datetime.datetime, value: int):
        pass

    async def incr(self, key: str, value: int):
        pass


class QueueAPI(ABC):
    """
    Interface for communication with queues.
    """

    async def get_message(self, queue_name: str) -> Tuple:
        pass

    async def get_light_statuses(self, queue_name: str):
        pass


class GraphiteStatsAPI(StatsAPI):
    GRAPHITE_HOST = "localhost"
    GRAPHITE_PORT = 2003

    def __init__(self):
        self.writer: Optional[asyncio.StreamWriter] = None
        self.reader: Optional[asyncio.StreamReader] = None

    async def _init_connection(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.GRAPHITE_HOST, self.GRAPHITE_PORT
        )

    async def _close_connection(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def gauge(self, key: str, event_time: int, value: int):
        await self._init_connection()
        message = f"{key} {value} {event_time}\n"
        self.writer.write(message.encode())
        await self.writer.drain()
        await self._close_connection()


class RedisQueueAPI(QueueAPI):
    CONNECTION_URL = "redis://localhost"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # most of magic methods cannot be asynchronous.
    def __init__(self):
        log.info('Connecting to redis')
        self.client = aioredis.from_url(self.CONNECTION_URL, decode_responses=True)

    async def consumer(self, queue_name: str):
        while True:
            message = await self.client.brpop(queue_name, timeout=0.01)
            if not message:
                break
            _, message = message
            yield message

    async def get_light_statuses(self, queue_name: str):
        log.info('Getting all records from queue')
        async for message in self.consumer(queue_name):
            yield self._reformat_message(message)

    @classmethod
    def _reformat_message(cls, message: str) -> Tuple:
        lights_status_change_time_str, status = message.split(',')
        try:
            lights_status_change_time = datetime.datetime.strptime(lights_status_change_time_str, cls.DATETIME_FORMAT)
            status = bool(int(status))
            return lights_status_change_time, status
        except ValueError:
            log.error(
                f"Can not cast these values - time: "
                f"{lights_status_change_time_str} of type {type(lights_status_change_time_str)}, "
                f"status: {status} of type {type(status)}", exc_info=True
            )


async def main():
    queue = RedisQueueAPI()
    stats = GraphiteStatsAPI()
    gauge_tasks = []
    async for time_of_change, status in queue.get_light_statuses(REDIS_QUEUE_NAME):
        gauge_tasks.append(
            asyncio.create_task(stats.gauge(GRAPHITE_METRIC_NAME, int(time_of_change.timestamp()), int(status)))
        )
    await asyncio.gather(*gauge_tasks)

if __name__ == "__main__":
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())

    # =================
    start = time.time()
    asyncio.run(main())
    print(f'Time of execution: {time.time() - start}')
