"""The application uses a real external RabbitMQ broker, never an in-memory queue."""
import json
import logging
import os
import threading

import pika

LOG = logging.getLogger(__name__)
QUEUE = "week06.jobs"
DLX = "week06.dead"
DLQ = "week06.jobs.dlq"
MAX_ATTEMPTS = 3


def connect():
    params = pika.URLParameters(os.getenv(
        "AMQP_URL", "amqp://classroom:classroom-local-only@localhost:5672/%2F"))
    params.heartbeat = 30
    params.socket_timeout = 3
    params.stack_timeout = 6
    params.blocked_connection_timeout = 5
    params.connection_attempts = 1
    return pika.BlockingConnection(params)


def topology(channel):
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind(queue=DLQ, exchange=DLX, routing_key="failed")
    channel.queue_declare(queue=QUEUE, durable=True, arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": "failed",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 20,
    })


class Publisher:
    def __init__(self, store):
        self.store = store
        # One process/dispatcher in this classroom setup. Each call owns its connection.
        self.lock = threading.Lock()

    def dispatch(self, id_):
        with self.lock:
            if not self.store.unsent(id_):
                return
            connection = connect()
            try:
                channel = connection.channel()
                topology(channel)
                channel.confirm_delivery()
                channel.basic_publish(exchange="", routing_key=QUEUE,
                    body=json.dumps({"job_id": id_}),
                    properties=pika.BasicProperties(delivery_mode=2,
                        content_type="application/json", message_id=id_),
                    mandatory=True)
                self.store.sent(id_)
                LOG.info("PUBLISH_CONFIRMED job=%s", id_)
            finally:
                if connection.is_open:
                    connection.close()
