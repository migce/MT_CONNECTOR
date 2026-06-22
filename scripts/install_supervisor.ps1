# -- MT5 Supervisor - Windows Startup Installer --
# Creates a shortcut in the user Startup folder so the supervisor
# starts automatically when you log in.
#
# Usage:
#   .\scripts\install_supervisor.ps1
#   .\scripts\install_supervisor.ps1 -Minimized   # start minimized
#
# To uninstall:
#   Remove-Item "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\MT5 Supervisor.lnk"

param(
    [switch]$Minimized
)

$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$python     = Join-Path $projectDir ".venv\Scripts\python.exe"
$startup    = [Environment]::GetFolderPath("Startup")
$lnk        = Join-Path $startup "MT5 Supervisor.lnk"

if (-not (Test-Path $python)) {
    Write-Host "ERROR: Python not found at $python" -ForegroundColor Red
    exit 1
}

$shell = New-Object -ComObject WScript.Shell
$sc = $shell.CreateShortcut($lnk)
$sc.TargetPath       = $python
$sc.Arguments         = "-m src.supervisor"
$sc.WorkingDirectory  = $projectDir
$sc.Description       = "MT5 Supervisor - auto-manages poller and trader"
if ($Minimized) {
    $sc.WindowStyle = 7   # minimized
}
$sc.Save()

Write-Host ""
Write-Host "  Shortcut created: $lnk" -ForegroundColor Green
Write-Host "  The supervisor will start automatically at next logon." -ForegroundColor Cyan
Write-Host "  It will auto-start poller + trader and monitor them." -ForegroundColor Cyan
Write-Host ""
