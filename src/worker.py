"""Manual ACK after durable business commit; bounded NACK and dead-letter paths."""
import json
import logging
import os
import time
from uuid import UUID

import pika

from src.api import JobInput
from src.broker import MAX_ATTEMPTS, QUEUE, connect, topology
from src.store import Store

LOG = logging.getLogger(__name__)


def handle(channel, method, properties, body, store, sleep=time.sleep):
    # Invalid envelopes are not retried; RabbitMQ routes rejected deliveries to DLQ.
    try:
        envelope = json.loads(body)
        if not isinstance(envelope, dict) or set(envelope) != {"job_id"}:
            raise ValueError("Invalid envelope")
        id_ = str(UUID(envelope["job_id"]))
        job = store.get(id_)
        if not job:
            raise ValueError("Unknown job")
        payload = JobInput.model_validate(job["payload"])
    except (ValueError, TypeError, KeyError, AttributeError):
        LOG.warning("NACK_DLQ malformed message")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    started = store.start(id_)
    if started is False:
        LOG.info("DUPLICATE_SKIP job=%s reports=%s", id_, store.effect_count(id_))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        LOG.info("ACK job=%s duplicate", id_)
        return
    if started is None:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        LOG.warning("NACK_DLQ already_failed job=%s", id_)
        return

    job = store.get(id_)
    LOG.info("CONSUME job=%s attempt=%s redelivered=%s",
             id_, job["attempts"], method.redelivered)
    try:
        if job["attempts"] > MAX_ATTEMPTS:
            raise ValueError("Retry allowance exhausted after repeated interruptions.")
        if payload.mode == "poison":
            raise ValueError("Demo poison payload cannot be processed.")
        if payload.mode == "retry_once" and job["attempts"] == 1:
            raise TimeoutError("Demo transient failure.")
        sleep(payload.duration)
        store.complete(id_)
    except (ValueError, TimeoutError) as error:
        terminal = isinstance(error, ValueError) or job["attempts"] >= MAX_ATTEMPTS
        store.problem(id_, str(error), terminal)
        if not terminal:
            sleep(min(2 ** job["attempts"], 4))
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=not terminal)
        LOG.warning("NACK_%s job=%s", "DLQ" if terminal else "REQUEUE", id_)
        return

    # A crash in this gap leaves a completed DB row and an unacked message.
    # Redelivery finds the terminal row and skips the already committed effect.
    channel.basic_ack(delivery_tag=method.delivery_tag)
    LOG.info("ACK job=%s completed reports=%s", id_, store.effect_count(id_))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store = Store(os.getenv("JOB_DB", "data/jobs.db"))
    store.init()
    while True:
        connection = None
        try:
            connection = connect()
            channel = connection.channel()
            topology(channel)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, auto_ack=False,
                on_message_callback=lambda ch, method, props, body: handle(
                    ch, method, props, body, store))
            LOG.info("WORKER_READY queue=%s prefetch=1 manual_ack=true", QUEUE)
            channel.start_consuming()
        except KeyboardInterrupt:
            LOG.info("WORKER_STOP; unacked work returns to broker")
            break
        except Exception as error:
            # Database/connection failures leave the delivery unacked.
            # Closing the channel requeues it. Broker delivery-limit bounds crashes.
            LOG.error("WORKER_RECONNECT reason=%s", type(error).__name__)
            time.sleep(2)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
