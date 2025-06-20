import time, datetime
from main import main

sleepSeconds = 300 # currently 5m, change to 12 hours (43200) when running on server
currentDateTime = datetime.datetime.now()
currentTime = currentDateTime.strftime("%Y-%m-%d %H-%M-%S")

def runDaemon():
    try:
        print(f"Running daemon at {currentTime}", flush=True)
        main()
    except Exception as e:
        print(f"Daemon crashed at {currentTime}: {e}", flush=True)
    time.sleep(sleepSeconds)

