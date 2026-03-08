# ============================================================
# Install MT5 Poller as a Windows Service using NSSM
# ============================================================
# NSSM (Non-Sucking Service Manager) must be installed:
#   choco install nssm  (or download from https://nssm.cc)
#
# Run this script as Administrator:
#   powershell -ExecutionPolicy Bypass -File scripts\install_poller.ps1
# ============================================================

param(
    [string]$ServiceName = "MT5Poller",
    [string]$PythonPath  = "",
    [string]$ProjectDir  = ""
)

# Resolve defaults
if (-not $ProjectDir) {
    $ProjectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
}

if (-not $PythonPath) {
    # Try to find python in the project's venv or system PATH
    $venvPython = Join-Path $ProjectDir ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $PythonPath = $venvPython
    } else {
        $PythonPath = (Get-Command python -ErrorAction SilentlyContinue).Source
    }
}

if (-not $PythonPath -or -not (Test-Path $PythonPath)) {
    Write-Error "Python not found. Specify -PythonPath or create a .venv."
    exit 1
}

Write-Host "=== MT5 Poller Service Installer ===" -ForegroundColor Cyan
Write-Host "Service Name : $ServiceName"
Write-Host "Python       : $PythonPath"
Write-Host "Project Dir  : $ProjectDir"
Write-Host ""

# Check NSSM
$nssm = Get-Command nssm -ErrorAction SilentlyContinue
if (-not $nssm) {
    Write-Error "NSSM not found. Install via: choco install nssm"
    exit 1
}

# Remove existing service if present
$existing = nssm status $ServiceName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Removing existing service '$ServiceName'..." -ForegroundColor Yellow
    nssm stop $ServiceName 2>$null
    nssm remove $ServiceName confirm
}

# Install
Write-Host "Installing service..." -ForegroundColor Green
nssm install $ServiceName $PythonPath "-m" "src.poller_main"
nssm set $ServiceName AppDirectory $ProjectDir
nssm set $ServiceName AppEnvironmentExtra "PYTHONPATH=$ProjectDir"
nssm set $ServiceName Description "MT5 Connector - Market Data Poller"
nssm set $ServiceName Start SERVICE_AUTO_START
nssm set $ServiceName AppStdout (Join-Path $ProjectDir "logs\poller_stdout.log")
nssm set $ServiceName AppStderr (Join-Path $ProjectDir "logs\poller_stderr.log")
nssm set $ServiceName AppRotateFiles 1
nssm set $ServiceName AppRotateBytes 10485760  # 10 MB

# Create logs directory
$logsDir = Join-Path $ProjectDir "logs"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

Write-Host ""
Write-Host "Service '$ServiceName' installed successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Commands:" -ForegroundColor Cyan
Write-Host "  nssm start $ServiceName    # Start the service"
Write-Host "  nssm stop  $ServiceName    # Stop the service"
Write-Host "  nssm status $ServiceName   # Check status"
Write-Host "  nssm edit  $ServiceName    # Edit settings (GUI)"
Write-Host "  nssm remove $ServiceName   # Remove the service"
