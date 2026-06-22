<#
.SYNOPSIS
    Creates a portable copy of the MT5 terminal for a trading account.
.DESCRIPTION
    The MetaTrader5 Python library is a singleton per process.
    To run multiple accounts simultaneously, each needs its own
    terminal folder launched with portable=True.

    This script copies the base terminal installation to a
    per-account folder under C:\MT5_Portable\<login>.
.PARAMETER Login
    MT5 account login number (used as folder name).
.PARAMETER BasePath
    Path to the base MT5 terminal installation.
    Default: "C:\Program Files\One Royal MT5 Terminal"
.EXAMPLE
    .\create_portable_terminal.ps1 -Login 5052841
    .\create_portable_terminal.ps1 -Login 5066472 -BasePath "C:\Program Files\My Broker MT5"
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$Login,

    [string]$BasePath = "C:\Program Files\One Royal MT5 Terminal"
)

$PortableRoot = "C:\MT5_Portable"
$TargetDir = Join-Path $PortableRoot $Login
$TargetExe = Join-Path $TargetDir "terminal64.exe"

if (Test-Path $TargetExe) {
    Write-Host "  [OK] Portable terminal already exists at: $TargetDir" -ForegroundColor Green
    Write-Host "  terminal64.exe: $TargetExe"
    exit 0
}

if (-not (Test-Path $BasePath)) {
    Write-Error "Base terminal not found at: $BasePath"
    exit 1
}

Write-Host "  Copying terminal to $TargetDir ..." -ForegroundColor Cyan
New-Item -Path $TargetDir -ItemType Directory -Force | Out-Null
Copy-Item -Path "$BasePath\*" -Destination $TargetDir -Recurse -Force

if (Test-Path $TargetExe) {
    Write-Host "  [OK] Portable terminal created." -ForegroundColor Green
    Write-Host "  Path: $TargetExe"
    Write-Host ""
    Write-Host "  Update the trading account mt5_path in the database:" -ForegroundColor Yellow
    Write-Host "    PATCH /api/v1/admin/accounts/<id>"
    Write-Host "    { `"mt5_path`": `"$TargetExe`" }"
} else {
    Write-Error "Copy failed - terminal64.exe not found in target."
    exit 1
}
