$ws = New-Object -ComObject WScript.Shell
$startup = [Environment]::GetFolderPath('Startup')
$sc = $ws.CreateShortcut("$startup\MT5 Poller Dashboard.lnk")
$sc.TargetPath = "C:\pyProjects\MT_Connector\.venv\Scripts\python.exe"
$sc.Arguments = "-m src.poller_main --dashboard"
$sc.WorkingDirectory = "C:\pyProjects\MT_Connector"
$sc.Description = "MT5 Poller with live dashboard (autostart)"
$sc.WindowStyle = 1
$sc.Save()
Write-Host "Autostart shortcut created in: $startup"
