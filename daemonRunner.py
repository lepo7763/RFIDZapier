import time, datetime
from main import main
from submissions import submissions

# start of daemon addon 
import sys, os, time, argparse, logging, daemon
from daemon import pidfile


sleepSeconds = 10 # 12 hours (43200) when running on server


def runDaemon():
    while True:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        try:
            print(f"Running daemon at {now}", flush=True)
            main()
            submissions()
            
        except Exception as e:
            print(f"Daemon crashed at {now}: {e}", flush=True)
        time.sleep(sleepSeconds)

if __name__ == "__main__":
    runDaemon()
