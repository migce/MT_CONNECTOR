$ws = New-Object -ComObject WScript.Shell
$desktop = [Environment]::GetFolderPath('Desktop')
$sc = $ws.CreateShortcut("$desktop\MT5 Poller.lnk")
$sc.TargetPath = "C:\pyProjects\MT_Connector\.venv\Scripts\python.exe"
$sc.Arguments = "-m src.poller_main"
$sc.WorkingDirectory = "C:\pyProjects\MT_Connector"
$sc.Description = "MT5 Poller"
$sc.WindowStyle = 1
$sc.Save()
Write-Host "Shortcut created on Desktop"
