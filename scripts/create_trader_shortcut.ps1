$ws = New-Object -ComObject WScript.Shell
$desktop = [Environment]::GetFolderPath('Desktop')
$sc = $ws.CreateShortcut("$desktop\MT5 Trader.lnk")
$sc.TargetPath = "C:\pyProjects\MT_Connector\scripts\start_trader.bat"
$sc.WorkingDirectory = "C:\pyProjects\MT_Connector"
$sc.Description = "MT5 Trader (deals, positions, account info)"
$sc.WindowStyle = 1
$sc.IconLocation = "shell32.dll,145"
$sc.Save()
Write-Host "Shortcut 'MT5 Trader' created on Desktop"
