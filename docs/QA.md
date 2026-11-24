# QA results and live checklist

## Executed in the authoring environment

**18 tests passed.** See test-results.txt. SQLite uses real temporary databases.
Publisher and RabbitMQ channels are test doubles.

Coverage:
- 202 after publication success, status polling, validation and missing IDs.
- Same key/payload reuses a job; conflicting reuse returns 409.
- Publication failure returns 503 and preserves the outbox.
- Lost ACK after commit followed by redelivery creates one report.
- Transient failure NACK/requeue then success; persistent attempt counter.
- Poison/malformed/unknown deliveries NACK without requeue.
- Repeated interruptions exhaust a finite retry allowance.
- Concurrent duplicate completions create one report.
- Outbox failure rolls back the new job.
- Publisher confirms, persistent messages and mandatory routing.
- Failed publication retains pending outbox state.
- Durable DLQ and quorum at-least-once dead-letter configuration.
- Worker registration uses manual ACK and prefetch one.

The test runner emitted one dependency deprecation warning about TestClient's
httpx adapter. It did not fail the suite.

audit-results.txt records the draft auto-ack flaw and corrected registration.
The static audit complements the behavioral ACK-gap test.

## NOT verified here

Docker, live RabbitMQ, broker restart persistence, AMQP redelivery, Windows
PowerShell scripts, live <100 ms timing and browser rendering/DevTools remain
unverified here. A browser launch attempt was blocked by socket restrictions.
Do not mark these passed based on unit tests.

## Laptop checklist

- [ ] Compose reaches healthy API and running worker.
- [ ] Dashboard loads; form/polling/mobile layout work without JS errors.
- [ ] Valid job returns 202 with unique ID and response-time header.
- [ ] Record client/server timing; verify the <100 ms target.
- [ ] UI remains usable; queued/processing becomes completed after work.
- [ ] Five rapid jobs produce distinct IDs and visible queue backlog.
- [ ] Kill an in-flight worker with docker compose stop -t 0 worker.
- [ ] Two submissions while stopped return 202.
- [ ] Restart recovers interrupted and queued jobs.
- [ ] Logs show CONSUME and ACK or DUPLICATE_SKIP/ACK.
- [ ] Transient scenario shows NACK_REQUEUE then ACK.
- [ ] Poison records failed and appears in week06.jobs.dlq.
- [ ] Broker restart preserves waiting messages (stop worker for this test).
- [ ] DevTools Network shows POST 202 and polling GET 200.
- [ ] Invalid input returns 422; missing ID returns 404.
- [ ] Preserve actual QA results and review evidence.
- [ ] Record face + screen together, 5:00–7:00.
- [ ] Publish repository and verify unlisted/public YouTube access.

scripts/qa.ps1 automates several live checks and saves JSON. It does not replace
manual browser and DLQ inspection.

## Observation log

- Date / machine:
- Startup:
- Tests:
- Timing (server/client):
- Burst Ready / Unacked:
- Interrupted job ID:
- Two offline-submission IDs:
- Recovery statuses:
- DLQ:
- Browser console / Network:
- Issues / fixes:
