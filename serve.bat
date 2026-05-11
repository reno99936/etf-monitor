@echo off
cd /d "C:\Users\Owner\reno-agent\etf-monitor"
start http://localhost:8080
python server.py
