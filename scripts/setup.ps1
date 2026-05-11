###############################################################################
# IEC 104 Demo – Deploy connectors and ksqlDB streams
# Run AFTER  docker compose up -d  and all services are healthy.
###############################################################################
$ErrorActionPreference = 'Stop'

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectDir = Split-Path -Parent $ScriptDir

$ConnectSource = "http://127.0.0.1:8083"
$ConnectSink   = "http://127.0.0.1:8084"
$KsqlDB        = "http://127.0.0.1:8088"

###############################################################################
# Helpers
###############################################################################
function Log($msg)  { Write-Host "[✓] $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "[!] $msg" -ForegroundColor Yellow }
function Fail($msg) { Write-Host "[✗] $msg" -ForegroundColor Red; exit 1 }

function Wait-ForUrl {
    param(
        [string]$Url,
        [string]$Name,
        [int]$MaxSeconds = 60
    )
    Write-Host "    Waiting for $Name ..." -NoNewline
    for ($i = 0; $i -lt $MaxSeconds; $i++) {
        try {
            Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 2 -ErrorAction Stop | Out-Null
            Write-Host " ready"
            return
        }
        catch {
            Write-Host "." -NoNewline
            Start-Sleep -Seconds 1
        }
    }
    Write-Host " TIMEOUT"
    Fail "$Name did not become ready within ${MaxSeconds}s"
}

###############################################################################
# 1 — Wait for all services
###############################################################################
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════"
Write-Host "  IEC 104 Demo – Setup"
Write-Host "═══════════════════════════════════════════════════════════"
Write-Host ""

Wait-ForUrl -Url "$ConnectSource/connectors" -Name "Connect-Source" -MaxSeconds 300
Wait-ForUrl -Url "$ConnectSink/connectors"   -Name "Connect-Sink"   -MaxSeconds 180
Wait-ForUrl -Url "$KsqlDB/info"              -Name "ksqlDB"         -MaxSeconds 90

###############################################################################
# 2 — Deploy IEC 104 Source Connector
###############################################################################
Write-Host ""
Log "Deploying IEC 104 Source Connector ..."

$SourceBody = Get-Content -Raw -Path "$ProjectDir\connectors\iec104-source.json"
try {
    $SourceResp = Invoke-WebRequest -Uri "$ConnectSource/connectors" `
        -Method Post `
        -ContentType "application/json" `
        -Body $SourceBody `
        -UseBasicParsing
    $SourceCode = $SourceResp.StatusCode
}
catch {
    $SourceCode = $_.Exception.Response.StatusCode.value__
}

if ($SourceCode -eq 201 -or $SourceCode -eq 200) {
    Log "Source connector created (HTTP $SourceCode)"
}
elseif ($SourceCode -eq 409) {
    Warn "Source connector already exists (HTTP 409)"
}
else {
    Fail "Source connector deployment failed (HTTP $SourceCode)"
}

###############################################################################
# 3 — Wait for the iec104 topic to exist (source connector must be producing)
###############################################################################
Write-Host ""
Write-Host "    Waiting for 'iec104' topic ..." -NoNewline
for ($i = 0; $i -lt 60; $i++) {
    try {
        $topics = docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>$null
        if ($topics -match '(?m)^iec104$') {
            Write-Host " found"
            break
        }
    } catch {}
    if ($i -eq 59) {
        Write-Host " TIMEOUT"
        Fail "Topic 'iec104' was not created within 60s"
    }
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 1
}

###############################################################################
# 4 — Deploy ksqlDB streams (flatten objects array)
###############################################################################
Write-Host ""
Log "Creating ksqlDB streams ..."

# Read the SQL file, strip comments, split on semicolons
$SqlRaw = Get-Content -Raw -Path "$ProjectDir\ksqldb\statements.sql"
$SqlRaw = $SqlRaw -replace '--[^\r\n]*', ''
$Statements = $SqlRaw -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }

foreach ($stmt in $Statements) {
    $singleLine = ($stmt -replace '\r?\n', ' ').Trim()
    $body = @{ ksql = "$singleLine;"; streamsProperties = @{} }
    $json = $body | ConvertTo-Json -Depth 5 -Compress

    try {
        $ksqlResp = Invoke-WebRequest -Uri "$KsqlDB/ksql" `
            -Method Post `
            -ContentType "application/vnd.ksql.v1+json; charset=utf-8" `
            -Body $json `
            -UseBasicParsing
        $ksqlCode = $ksqlResp.StatusCode
        $ksqlBody = $ksqlResp.Content
    }
    catch {
        $ksqlCode = $_.Exception.Response.StatusCode.value__
        $ksqlBody = ""
        try { $ksqlBody = $_.ErrorDetails.Message } catch {}
    }

    if ($ksqlCode -ge 200 -and $ksqlCode -lt 300) {
        Log "  ksqlDB statement OK (HTTP $ksqlCode)"
    }
    elseif ($ksqlBody -match "(?i)already exists") {
        Warn "  ksqlDB stream already exists"
    }
    else {
        $snippet = if ($ksqlBody.Length -gt 200) { $ksqlBody.Substring(0, 200) } else { $ksqlBody }
        Warn "  ksqlDB response (HTTP $ksqlCode): $snippet"
    }
}

###############################################################################
# 5 — Wait briefly for the IEC104_FLAT topic to be created by ksqlDB
###############################################################################
Write-Host ""
Log "Waiting 10s for ksqlDB to materialize IEC104_FLAT topic ..."
Start-Sleep -Seconds 10

###############################################################################
# 6 — Deploy JDBC Sink Connector
###############################################################################
Write-Host ""
Log "Deploying JDBC Sink Connector ..."

$SinkBody = Get-Content -Raw -Path "$ProjectDir\connectors\jdbc-sink.json"
try {
    $SinkResp = Invoke-WebRequest -Uri "$ConnectSink/connectors" `
        -Method Post `
        -ContentType "application/json" `
        -Body $SinkBody `
        -UseBasicParsing
    $SinkCode = $SinkResp.StatusCode
}
catch {
    $SinkCode = $_.Exception.Response.StatusCode.value__
}

if ($SinkCode -eq 201 -or $SinkCode -eq 200) {
    Log "Sink connector created (HTTP $SinkCode)"
}
elseif ($SinkCode -eq 409) {
    Warn "Sink connector already exists (HTTP 409)"
}
else {
    Fail "Sink connector deployment failed (HTTP $SinkCode)"
}

###############################################################################
# 7 — Summary
###############################################################################
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════"
Write-Host "  Setup complete!"
Write-Host "═══════════════════════════════════════════════════════════"
Write-Host ""
Write-Host "  Kafka Connect Source : $ConnectSource/connectors"
Write-Host "    └─ Source status : $ConnectSource/connectors/iec104-source/status"
Write-Host "  Kafka Connect Sink   : $ConnectSink/connectors"
Write-Host "    └─ Sink status   : $ConnectSink/connectors/jdbc-sink/status"
Write-Host "  ksqlDB               : $KsqlDB/info"
Write-Host "  PostgreSQL           : localhost:5432  (db: iec104, user: iec104)"
Write-Host "  Grafana              : http://localhost:3000"
Write-Host "  IEC104 Simulator UI  : http://localhost:4300"
Write-Host "  IEC104 Simulator API : http://localhost:8080"
Write-Host ""

# To inspect the database manually:
#   docker exec -it postgres psql -U iec104 -d iec104
#   SELECT "COMMONADDRESS", "IOA", "TYPENAME", COUNT(*) FROM iec104_measurements GROUP BY 1,2,3 ORDER BY 1,2;
#   SELECT * FROM iec104_measurements WHERE "COMMONADDRESS" = 1 ORDER BY inserted_at DESC LIMIT 20;
