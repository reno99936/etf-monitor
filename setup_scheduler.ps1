# 建立 Windows 工作排程：每週一至週五台北 19:30 執行 ETF 監控
$taskName = "ETF Monitor Daily"
$batPath   = "C:\Users\Owner\reno-agent\etf-monitor\run.bat"

$action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batPath`""
$trigger = New-ScheduledTaskTrigger -Weekly `
    -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday `
    -At "19:30"
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

# 若已存在則更新
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask `
    -TaskName $taskName `
    -Action  $action `
    -Trigger $trigger `
    -Settings $settings `
    -Force | Out-Null

Write-Host "✅ 排程已建立：$taskName"
Write-Host "   執行時間：週一至週五 19:30"
Write-Host "   腳本路徑：$batPath"
Write-Host "   記錄檔案：C:\Users\Owner\reno-agent\etf-monitor\logs\fetch.log"
