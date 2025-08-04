import logging.handlers
import sys, os, time, datetime, win32serviceutil, win32service, win32event, servicemanager, logging, traceback, threading
from main import mainFunction
from submissions import submissionsFunction


sleepSeconds = 43200 # 12 hours (43200) when running on server

class myDaemon(win32serviceutil.ServiceFramework):
    _svc_name_ = "myDaemon"
    _svc_display_name_ = "My Python Daemon"
    _svc_description_ = "Runs RFID exclusions and submissions"

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)

    # window service callbacks
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        if self.worker and self.worker.is_alive():
            self.worker.join(30)

    def _worker_entry(self):
        try:
            self.runDaemon()
        except Exception:
            logging.exception("Worker thread crashed, stopping service")
            win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)

        self.worker = threading.Thread(target=self._worker_entry, daemon=True)
        self.worker.start()

        self.ReportServiceStatus(win32service.SERVICE_RUNNING)

        win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
        if self.worker.is_alive():
            self.worker.join(10)

    def _configure_logging(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        path = fr"C:\Users\Ranfe\Music\RFIDZapier\daemonLogger" # TODO: change to C:\programdata\...
        os.makedirs(path, exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(path, f"daemon {now}.log"),
            when="midnight", 
            backupCount=14 # keeping two weeks
            )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.addHandler(handler)
            

    # function to do work
    def runDaemon(self):
        self._configure_logging() 
        logging.info("Service started")
        
        def runCycle():
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                logging.info("""\n\n\n------------------------------\nExclusion cycle start %s\n------------------------------\n\n\n""", now)
                mainFunction()
            except Exception:
                logging.exception("Exclusion crashed")
            
            try:
                logging.info("""\n\n\n------------------------------\nSubmissions cycle start %s\n------------------------------\n\n\n""", now)
                submissionsFunction()
            except Exception:
                logging.exception("Submissions crashed")
            logging.info(f"\n\n-------------------------------------------------\nService ran at {now}. Will run again in 12 hours...\n-------------------------------------------------\n\n")

        runCycle() # runs instantly when the daemon is called
        while True:
            rc = win32event.WaitForSingleObject(self.stop_event, sleepSeconds * 1000)
                
            if rc == win32event.WAIT_OBJECT_0: # stop requested
                break
            runCycle() # runs every 12 hours

        logging.info("Service Stopped")



if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(myDaemon)