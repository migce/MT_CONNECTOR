# ============================================================
# fix_mt5_ipc.ps1  -- Fix MT5 IPC hang / poller not connected
#
# Steps:
#   1. Kill all hung terminal64.exe processes
#   2. Restart poller via supervisor REST API
#   3. Wait up to WaitSec for mt5_connected = true
#   4. Print final status (health + poller/status)
#
# Usage:
#   .\scripts\fix_mt5_ipc.ps1
#   .\scripts\fix_mt5_ipc.ps1 -SupervisorPort 9100 -ApiPort 9000
#   .\scripts\fix_mt5_ipc.ps1 -SkipKill        # skip killing terminals
#   .\scripts\fix_mt5_ipc.ps1 -RestartAll       # restart poller + trader
# ============================================================

param(
    [int]    $SupervisorPort = 9100,
    [int]    $ApiPort        = 9000,
    [switch] $SkipKill,
    [switch] $RestartAll,
    [int]    $WaitSec        = 90
)

$ErrorActionPreference = "Stop"

function Write-Step { param($msg) Write-Host "`n[STEP] $msg" -ForegroundColor Cyan }
function Write-OK   { param($msg) Write-Host "  [OK] $msg"   -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "  [!!] $msg"   -ForegroundColor Yellow }
function Write-Fail { param($msg) Write-Host " [ERR] $msg"   -ForegroundColor Red }

$supervisorBase = "http://localhost:$SupervisorPort"
$apiBase        = "http://localhost:$ApiPort"

# -----------------------------------------------------------
# 0. Check supervisor is alive
# -----------------------------------------------------------
Write-Step "Checking supervisor at $supervisorBase/status"
try {
    $svStatus = Invoke-RestMethod -Uri "$supervisorBase/status" -TimeoutSec 5
    Write-OK "Supervisor is up. Services:"
    foreach ($svc in $svStatus.services) {
        $alive = if ($svc.alive) { "alive" } else { "DEAD" }
        Write-Host ("    {0,-10} pid={1,-6} {2,-5} restarts={3}" -f $svc.name, $svc.pid, $alive, $svc.restarts)
    }
} catch {
    Write-Fail "Supervisor not responding on port $SupervisorPort. Start it manually:"
    Write-Host "    .\.venv\Scripts\python.exe -m src.supervisor" -ForegroundColor Yellow
    exit 1
}

# -----------------------------------------------------------
# 1. Kill hung terminal64.exe processes
# -----------------------------------------------------------
if (-not $SkipKill) {
    Write-Step "Killing hung terminal64.exe processes"
    $terminals = Get-Process -Name "terminal64" -ErrorAction SilentlyContinue
    if ($terminals) {
        foreach ($p in $terminals) {
            Write-Warn "Killing terminal64.exe  PID=$($p.Id)  Path=$($p.Path)"
            Stop-Process -Id $p.Id -Force
        }
        Write-OK "Killed $($terminals.Count) process(es). Waiting 3s..."
        Start-Sleep -Seconds 3
    } else {
        Write-OK "No running terminal64.exe found."
    }
} else {
    Write-Warn "-SkipKill: skipping terminal kill"
}

# -----------------------------------------------------------
# 2. Restart poller (and optionally trader) via supervisor
# -----------------------------------------------------------
Write-Step "Restarting poller via supervisor"
try {
    $r = Invoke-RestMethod -Method Post -Uri "$supervisorBase/services/poller/restart" -TimeoutSec 10
    Write-OK "poller restart: $($r | ConvertTo-Json -Compress)"
} catch {
    Write-Fail "Failed to restart poller: $_"
    exit 1
}

if ($RestartAll) {
    Write-Step "Restarting trader via supervisor"
    try {
        $r = Invoke-RestMethod -Method Post -Uri "$supervisorBase/services/trader/restart" -TimeoutSec 10
        Write-OK "trader restart: $($r | ConvertTo-Json -Compress)"
    } catch {
        Write-Warn "Failed to restart trader: $_"
    }
}

# -----------------------------------------------------------
# 3. Wait for mt5_connected == true
# -----------------------------------------------------------
Write-Step "Waiting for mt5_connected=true (timeout ${WaitSec}s)"
$deadline  = (Get-Date).AddSeconds($WaitSec)
$connected = $false

while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    try {
        $h = Invoke-RestMethod -Uri "$apiBase/api/v1/health" -TimeoutSec 5
        $mark = if ($h.mt5_connected) { "[OK]" } else { "[ ]" }
        Write-Host ("  {0}  mt5_connected={1,-5} trader_connected={2,-5}  {3}" -f `
            $mark, $h.mt5_connected, $h.trader_connected, (Get-Date -Format 'HH:mm:ss'))
        if ($h.mt5_connected) {
            $connected = $true
            break
        }
    } catch {
        Write-Host "  [?]  /health unavailable: $_  $(Get-Date -Format 'HH:mm:ss')"
    }
}

# -----------------------------------------------------------
# 4. Final report
# -----------------------------------------------------------
Write-Step "Final status"

if (-not $connected) {
    Write-Fail "mt5_connected did NOT become true within ${WaitSec}s."
    Write-Warn "Possible causes:"
    Write-Host "  - terminal64.exe cannot reach broker (network/VPN down)"
    Write-Host "  - wrong login/password/server in .env  (MT5_LOGIN, MT5_PASSWORD, MT5_SERVER)"
    Write-Host "  - wrong terminal path  (MT5_PATH)"
    Write-Host "  - check poller logs:  .\.venv\Scripts\python.exe -m src.poller_main --dashboard"
    exit 1
}

Write-OK "MT5 connected!"

# Poller snapshot
try {
    $ps = Invoke-RestMethod -Uri "$apiBase/api/v1/poller/status" -TimeoutSec 5
    $symCount = if ($ps.symbols) { $ps.symbols.Count } else { 0 }
    Write-OK "Poller snapshot ready. Symbols: $symCount"
    if ($ps.symbols) {
        foreach ($sym in ($ps.symbols | Select-Object -First 8)) {
            Write-Host ("    {0,-12} bid={1}  ask={2}  time={3}" -f $sym.symbol, $sym.bid, $sym.ask, $sym.time)
        }
    }
} catch {
    Write-Warn "Poller /status not ready yet: $_"
}

# Recent candles from DB
Write-Step "Recent candles in DB (via API)"
$checkSymbols = @("EURUSD","USDJPY","AUDUSD","XAUUSD","BTCUSD")
foreach ($sym in $checkSymbols) {
    try {
        $candles = Invoke-RestMethod -Uri "$apiBase/api/v1/candles/$sym/M1?limit=1" -TimeoutSec 5
        if ($candles -and $candles.Count -gt 0) {
            Write-Host ("    {0,-12} last_candle={1}" -f $sym, $candles[0].time)
        }
    } catch {
        # symbol may not be available - skip silently
    }
}

Write-Host ""
Write-OK "Done. Quotes should now be updating."
