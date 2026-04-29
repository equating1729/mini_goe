import schedule
import time
import sys
import os

sys.path.append(os.path.dirname(__file__))
from fetch import run_ingestion

run_ingestion()

schedule.every(4).hours.do(run_ingestion)

print("\nScheduler running. Press Ctrl+C to stop.")
while True:
    schedule.run_pending()
    time.sleep(60)