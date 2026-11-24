# Windows PowerShell 5.1. Run from the extracted project directory.
# This script creates demo jobs and briefly stops/restarts this project's worker.
$ErrorActionPreference = "Stop"
$base = "http://localhost:8006"
$timings = @()
$checks = @()

function Assert-That($condition, $message) {
    if (-not $condition) { throw $message }
    Write-Host "PASS: $message" -ForegroundColor Green
}
function New-Job($title, $mode = "normal") {
    $body = @{title=$title; duration=4; mode=$mode} | ConvertTo-Json
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-WebRequest -UseBasicParsing -Uri "$base/api/jobs" -Method Post -ContentType "application/json" -Headers @{"Idempotency-Key"=[guid]::NewGuid().ToString()} -Body $body
    $watch.Stop()
    Assert-That ($response.StatusCode -eq 202) "Submission returned HTTP 202"
    $script:timings += [math]::Round($watch.Elapsed.TotalMilliseconds, 2)
    return ($response.Content | ConvertFrom-Json).id
}
function Get-Job($id) { return Invoke-RestMethod -Uri "$base/api/jobs/$id" }
function Wait-Jobs($ids, $expected = "completed") {
    $deadline = (Get-Date).AddSeconds(90)
    do {
        $states = @($ids | ForEach-Object { Get-Job $_ })
        if (@($states | Where-Object { $_.status -ne $expected }).Count -eq 0) { return }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $expected : $($states | ConvertTo-Json -Compress)"
}
function Show-Queues {
    docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect RabbitMQ queues" }
}

docker compose ps
$id = New-Job "QA single report"
$initial = Get-Job $id
Assert-That ($initial.status -in @("queued","processing")) "Initial response precedes report completion"
Wait-Jobs @($id)
$checks += "Single job transitioned to completed"

$burst = @(1..5 | ForEach-Object { New-Job "QA burst $_" })
Assert-That (@($burst | Select-Object -Unique).Count -eq 5) "Five unique job IDs"
Show-Queues
Wait-Jobs $burst
$checks += "Burst of five drained"

# Ensure there is a delivery in flight before killing the worker.
$crash = New-Job "QA interrupted delivery"
$deadline = (Get-Date).AddSeconds(15)
do {
    $job = Get-Job $crash
    if ($job.status -eq "processing") { break }
    Start-Sleep -Milliseconds 100
} while ((Get-Date) -lt $deadline)
Assert-That ($job.status -eq "processing") "Worker has an in-flight delivery"
try {
    docker compose stop -t 0 worker
    if ($LASTEXITCODE -ne 0) { throw "Worker stop failed" }
    $a = New-Job "QA while worker stopped A"
    $b = New-Job "QA while worker stopped B"
    Assert-That ((Get-Job $a).status -eq "queued") "Ingestion works without worker"
    Assert-That ((Get-Job $b).status -eq "queued") "Second job remains queued"
    Show-Queues
} finally {
    docker compose start worker
    if ($LASTEXITCODE -ne 0) { throw "Worker restart failed" }
}
Wait-Jobs @($crash, $a, $b)
$checks += "Interrupted delivery and two offline submissions recovered"

$retry = New-Job "QA transient error" "retry_once"
Wait-Jobs @($retry)
Assert-That ((Get-Job $retry).attempts -eq 2) "Transient failure retried once"
$poison = New-Job "QA poison payload" "poison"
Wait-Jobs @($poison) "failed"
$checks += "Transient retry and poison rejection"
Show-Queues

$under100 = @($timings | Where-Object { $_ -ge 100 }).Count -eq 0
$report = @{
    timestamp=(Get-Date).ToString("o")
    checks=$checks
    browser_network_inspection="Manual check still required"
    response_ms=$timings
    all_submissions_under_100ms=$under100
    caveat="Client round-trip measurements. Inspect failed latency targets before recording."
}
$report | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 qa-live-results.json
Write-Host ($report | ConvertTo-Json -Depth 6)
if (-not $under100) {
    Write-Warning "At least one submission exceeded 100 ms. Warm the broker and investigate/retest. Do not claim the latency target passed."
}
Write-Host "Saved qa-live-results.json. Inspect DevTools and the DLQ manually before submission."
