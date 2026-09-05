# Explicit installer. Does not start/stop any Poller, Trader or MT5 terminal.
param([switch]$Start)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$taskName = 'MT5 History Worker'
$python = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw 'History Python environment missing' }
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    if ($existing.State.ToString() -ne 'Disabled' -or $existing.Actions.Count -ne 1 -or
        $existing.Actions[0].Execute -ne $python -or $existing.Actions[0].Arguments -ne '-m src.history_main' -or
        $existing.Actions[0].WorkingDirectory -ne $root) { throw 'Existing history task does not match the disabled rollback fixture' }
    Enable-ScheduledTask -TaskName $taskName | Out-Null
    if ($Start) { Start-ScheduledTask -TaskName $taskName }
    Write-Output 'Verified disabled history task enabled'
    exit 0
}
$action = New-ScheduledTaskAction -Execute $python -Argument '-m src.history_main' -WorkingDirectory $root
$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$trigger = New-ScheduledTaskTrigger -AtLogOn -User ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
Register-ScheduledTask -TaskName $taskName -Action $action -Principal $principal -Settings $settings -Trigger $trigger | Out-Null
if ($Start) { Start-ScheduledTask -TaskName $taskName }
Write-Output 'History task installed: interactive logon, single instance, at most 3 failure retries'
