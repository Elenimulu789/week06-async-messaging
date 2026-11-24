# AI prompt log and messaging audit

## User request

The user supplied the Week 06 assignment and wrote:

> yes start week6

## Expanded design brief

This specification was composed by ChatGPT to implement the request. It was
not a separate prompt typed by the student:

> Build a local FastAPI report-job producer, RabbitMQ broker, separate Python
> consumer and polling HTML dashboard. Return 202 after broker acceptance with
> a job ID. Simulate four seconds of work. Include durable queues, persistent
> publication and confirms, manual ACK after commit, bounded retry, a DLQ,
> duplicate-effect protection, Docker Compose, Windows commands, an AI audit,
> and a five-to-seven-minute recording script. Distinguish real broker checks
> from mocked tests and do not attribute unperformed work to the student.

## Initial artifact and flaw

The first simplified draft is retained at audit/initial_worker.py.txt. It is
a non-executable audit fragment, not an alternative worker to run:

~~~python
channel.basic_consume(
    queue="week06.jobs",
    on_message_callback=consume,
    auto_ack=True,
)
~~~

**Failure pathway:** the broker considers delivery complete before the report
transaction finishes. Killing the worker in that gap loses work recovery.
The fragment also omitted retry and poison handling.

ChatGPT identified and corrected this during the session. This is AI-assisted
review, not a claim that the student manually patched the code.

## Correction

src/worker.py uses auto_ack=False and prefetch_count=1. Its callback commits
the report and completed status before basic_ack. Expected transient failures
use bounded NACK/requeue. Permanent failures use NACK without requeue to the DLQ.

reports.job_id is unique. A regression test raises a connection error at ACK,
then redelivers the message and checks that only one report exists.

src/broker.py uses publisher confirms, mandatory routing and persistent
delivery. src/store.py commits jobs and outbox entries together. Unconfirmed
publication stays pending instead of falsely returning 202.

## Verification

- scripts/audit_ack.py flags the draft setting and checks final manual ACK.
- docs/test-results.txt records automated test results.
- Tests use real temporary SQLite and broker/channel doubles.
- They do not establish live RabbitMQ behavior, Docker persistence, or <100 ms
  latency.
- Docker/RabbitMQ are unavailable in the authoring environment.
- A browser launch was blocked by socket restrictions, so visual browser checks
  remain pending.
- Add the student's actual review, changes, live observations and repository
  URL as they happen.
