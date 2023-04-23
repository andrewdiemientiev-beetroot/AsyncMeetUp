import socket
import datetime
import time
from typing import Tuple, List
from abc import ABC
import logging

import redis

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

REDIS_QUEUE_NAME = 'map_sync_home'
GRAPHITE_METRIC_NAME = 'local.home.sync'


class StatsAPI(ABC):
    """
    Interface for communication with time-series databases
    """

    def gauge(self, key: str, event_time: datetime.datetime, value: int):
        pass

    def incr(self, key: str, value: int):
        pass


class QueueAPI(ABC):
    """
    Interface for communication with queues.
    """

    def get_message(self, queue_name: str) -> Tuple:
        pass

    def get_light_statuses(self, queue_name: str) -> List[Tuple]:
        pass


class GraphiteStatsAPI(StatsAPI):
    GRAPHITE_HOST = "localhost"
    GRAPHITE_PORT = 2003

    def gauge(self, key: str, event_time: int, value: int):
        message = f"{key} {value} {event_time}\n"
        self._send_to_graphite(message)

    # Blocking IO
    def _send_to_graphite(self, message: str):
        time.sleep(0.05)
        sock = socket.socket()
        sock.connect((self.GRAPHITE_HOST, self.GRAPHITE_PORT))
        sock.send(message.encode())
        sock.close()

    def bulk_gauge(self, data: List[Tuple]):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.GRAPHITE_HOST, self.GRAPHITE_PORT))
        for event_time, status in data:
            event_time = int(event_time.timestamp())
            key = 'local.home.sync'
            value = int(status)
            message = f"{key} {value} {event_time}\n"
            sock.sendall(message.encode())
        sock.close()


class RedisQueueAPI(QueueAPI):
    CONNECTION_URL = "redis://localhost"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        log.info('Connecting to redis')
        self.client = redis.Redis.from_url(self.CONNECTION_URL, decode_responses=True)

    def get_light_statuses(self, queue_name: str) -> List[Tuple]:
        log.info('Getting all records from queue')
        light_statuses = self.get_messages(queue_name)
        formatted_light_statuses = [
            self._reformat_message(event_time, status) for event_time, status in light_statuses.items()
        ]
        log.info('Reformatting results')
        return formatted_light_statuses

    # Blocking IO
    def get_messages(self, queue_name: str) -> dict:
        messages = self.client.hgetall(queue_name)
        self.client.flushall()
        return messages

    # CPU bound operation
    def _reformat_message(self, light_status_change_time: str, status: str) -> Tuple:
        try:
            light_status_change_time = datetime.datetime.strptime(light_status_change_time, self.DATETIME_FORMAT)
            status = bool(int(status))
        except ValueError:
            log.error(
                f"Can not cast these values - time: {light_status_change_time} "
                f"of type {type(light_status_change_time)}, status: {status} of type {type(status)}", exc_info=True
            )
        return light_status_change_time, status


def send_data_to_graphite(client: GraphiteStatsAPI, data: List[Tuple]):
    for event_time, status in data:
        client.gauge('local.home.sync', int(event_time.timestamp()), int(status))


def main():
    queue = RedisQueueAPI()
    stats = GraphiteStatsAPI()

    light_status_events = queue.get_light_statuses(REDIS_QUEUE_NAME)
    print(f"Number of data: {len(light_status_events)}")
    send_data_to_graphite(stats, light_status_events)


if __name__ == "__main__":
    start = time.time()
    main()
    print(f'Time of execution: {time.time() - start}')
