import time, datetime
from main import main
from submissions import submissions

sleepSeconds = 10 # 12 hours (43200) when running on server


def runDaemon():
    while True:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        try:
            print(f"Running daemon at {now}", flush=True)
            submissions()
            
        except Exception as e:
            print(f"Daemon crashed at {now}: {e}", flush=True)
        time.sleep(sleepSeconds)

if __name__ == "__main__":
    runDaemon()