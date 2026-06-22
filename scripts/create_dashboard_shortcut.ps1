$ws = New-Object -ComObject WScript.Shell
$desktop = [Environment]::GetFolderPath('Desktop')
$sc = $ws.CreateShortcut("$desktop\MT5 Poller Dashboard.lnk")
$sc.TargetPath = "C:\pyProjects\MT_Connector\.venv\Scripts\python.exe"
$sc.Arguments = "-m src.poller_main --dashboard"
$sc.WorkingDirectory = "C:\pyProjects\MT_Connector"
$sc.Description = "MT5 Poller with live dashboard"
$sc.WindowStyle = 1
$sc.Save()
Write-Host "Dashboard shortcut created on Desktop"
