# Week 06 recording script

Target **6:30**. These are rehearsal targets. Check the actual video is 5–7
minutes with webcam and screen visible throughout. Narrate naturally and point
to evidence; do not claim unobserved results.

## Before recording

Start Docker Desktop. Inside the extracted project folder:

~~~powershell
docker compose up --build -d
~~~

Open http://localhost:8006 and http://localhost:15672.
RabbitMQ login: classroom / classroom-local-only.
Open **Queues and Streams**. Keep README, ai_prompt_log.md and docs/QA.md ready.
Open dashboard DevTools > Network with the status column visible.

In a second PowerShell window:

~~~powershell
docker compose logs -f api worker
~~~

Use a third window for stop/start commands. Rehearse once before recording.
Run automated live QA separately, not during this timed demonstration.

## 0:00–1:30 | Architecture and AI audit

**Show /architecture, then ai_prompt_log.md.**

“Today I’m demonstrating asynchronous report generation. A client sends a
request to a FastAPI producer. The producer saves the job and an outbox entry,
publishes to RabbitMQ, and returns 202 after broker confirmation. It does not
wait for the report to finish.

A separate worker consumes the job, simulates four seconds of processing, and
commits the result to SQLite. The dashboard polls the API for status.
RabbitMQ holds the work between these two processes.

ChatGPT generated the implementation and assisted with its audit. The retained
initial worker draft used auto_ack equals true. That would let the broker
consider delivery complete before the report transaction finished. A worker
crash in that gap could lose the work.

The corrected worker uses manual acknowledgments after the database commit.
It also includes bounded retries, a dead-letter queue and duplicate protection.
My task is to review and verify those behaviors on this machine.”

## 1:30–3:15 | Live submission and polling

**Show dashboard and DevTools. Submit one Normal report.**

“I’m submitting a report. The dashboard shows HTTP 202 and the browser
round-trip measurement. Network also shows an X-Response-Time-Ms header for
server handling. I’m comparing the actual measurements with the assignment’s
hundred-millisecond target.

The job has its own identifier. Its status is initially queued or processing
while the page remains usable. After the worker finishes, polling shows
completed and displays the report result.

PUBLISH_CONFIRMED is a producer log. CONSUME and ACK belong to the worker.
Publication confirmation means broker acceptance; the consumer ACK follows
successful processing. They protect different boundaries.

Prefetch one means this worker holds at most one unacknowledged delivery.
The status comes from SQLite and may change between polling requests.”

**Show completion and matching ACK. If timing exceeds the target, state the
actual result and note it for investigation instead of calling it a pass.**

## 3:15–5:15 | Burst and crash recovery

**Click Queue 5 reports. Show RabbitMQ Ready and Unacked counts.**

“Now five requests arrive rapidly. RabbitMQ buffers excess work. Ready messages
wait for consumption; Unacked shows a delivery currently held by a worker.
The API remains available while the queue drains.”

**While a job says processing, run:**

~~~powershell
docker compose stop -t 0 worker
~~~

“This stops the worker immediately so I can test an interrupted delivery.
The API and broker remain running.”

**Submit two more Normal reports. Show 202s and waiting queue counts.**

“These two requests are accepted while the worker is stopped. Their jobs stay
queued. Request ingestion is separated from processing availability.”

**Restart:**

~~~powershell
docker compose start worker
~~~

“The restarted worker can pick up waiting jobs and the unacknowledged delivery.
I’m checking the original job identifiers reach completed and matching them
with the worker logs.

Duplicate delivery can still happen if a worker crashes after committing its
result but before ACK. The report table uses the job ID as a unique key, and
completed work is skipped on redelivery. That protects this database effect
from duplication.”

**Wait for completion. Show polling GET 200 and browser console. Use part of
the next section if the final jobs need a few more seconds.**

## 5:15–6:30 | QA, DLQ and trade-offs

**Show QA checklist/test output. Submit Poison payload. Show failed status,
NACK_DLQ log and the DLQ count in RabbitMQ.**

“The automated tests simulate a lost ACK after commit and verify that
redelivery creates one report. Those tests use broker doubles. The live
RabbitMQ observations I just demonstrated are separate verification.

This poison scenario represents work that cannot be processed. The worker
records failed and rejects it without requeueing. RabbitMQ routes it to the
dead-letter queue for inspection instead of retrying forever. A transient
error uses a bounded retry instead.

Asynchronous messaging absorbs bursts and allows ingestion during a worker
outage. The cost is eventual consistency, duplicate deliveries, retry policies
and operational monitoring.

The outbox preserves publication intent if the API crashes between committing
the job and publishing it. Recovery can publish duplicates, so idempotency
still matters.

I would keep immediate validation synchronous and put report generation behind
the queue. This single-node setup demonstrates restart persistence, not
production high availability.”

## Tests before recording

~~~powershell
docker compose exec api python -m pytest -q
docker compose exec api python scripts/audit_ack.py
~~~

Optional live QA outside recording:

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\qa.ps1
~~~

## Canvas submission

### Week 06 Hands-On Assignment Submission

- **Live Demo URL:** N/A — Local Docker Architecture
- **GitHub Repository:** [Paste your actual published repository URL]
- **Video Demonstration:** [Paste verified Unlisted/Public YouTube URL]

### Timestamps

Update to match your recording:

- 0:00 — Architecture and AI Messaging Audit
- 1:30 — Asynchronous Submission and 202 Accepted
- 3:15 — Queue Buffering and Worker Crash Recovery
- 5:15 — QA, Dead-Letter Queue, and Trade-offs
