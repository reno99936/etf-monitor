@echo off
cd /d "C:\Users\Owner\reno-agent\etf-monitor"
if not exist logs mkdir logs
set PYTHONUTF8=1
echo [%date% %time%] 開始執行 >> logs\fetch.log
python fetch_data.py >> logs\fetch.log 2>&1
echo [%date% %time%] 執行結束 >> logs\fetch.log
