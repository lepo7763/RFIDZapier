import logging.handlers
import sys, os, time, datetime, win32serviceutil, win32service, win32event, servicemanager, logging, traceback
from main import mainFunction
from submissions import submissionsFunction
sleepSeconds = 43200 # 12 hours (43200) when running on server


class myDaemon(win32serviceutil.ServiceFramework):
    _svc_name_ = "myDaemon"
    _svc_display_name_ = "My Python Daemon"
    _svc_description_ = "Logs a tick every 5s - demo"

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)

    # window service callbacks
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)

        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )

        # add a windows notification for when the thingy crashes
        try:   
            self.runDaemon()
        except Exception:
            servicemanager.LogErrorMsg(
                f"{self._svc_name_} crashed:\n{traceback.format_exc()}"
            )

    def _configure_logging(self):
        path = fr"C:\Users\Ranfe\Music\RFIDZapier\daemonLogger" # TODO: change to C:\programdata\...
        os.makedirs(path, exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(path, "daemon.log"),
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
                logging.info("Exclusion cycle start %s", now)
                mainFunction()
            except Exception:
                logging.exception("Exclusion crashed")
            
            try:
                logging.info("Submission cycle start %s", now)
                submissionsFunction()
            except Exception:
                logging.exception("Submissions crashed")
            
        while True:
            rc = win32event.WaitForSingleObject(self.stop_event, sleepSeconds * 1000)
                
            if rc == win32event.WAIT_OBJECT_0: # stop requested
                break
            runCycle()

        logging.info("Service Stopped")
        



        """ old:
        logTime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = fr"C:\Users\Ranfe\Music\RFIDZapier\daemonLogger\myDaemon_{logTime}.log" # TODO: change to C:\programdata\...
        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.basicConfig(
            filename=path,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )

        logging.info("service started")
        # Loop here until SCM asks to stop
        while win32event.WaitForSingleObject(self.stop_event, 0) == win32event.WAIT_TIMEOUT:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # run exclusions
            try:
                logging.info(f"Taking new submissions for exclusion at {now}")
                mainFunction() 
            except Exception as e:
                logging.exception("Exclusion crashed: %s", e)

            # run submissions
            try:
                logging.info(f"Taking in new submission values from general_form_sub table at {now}") 
                submissionsFunction() 
            except Exception as e:
                logging.exception("Submissions crashed: %s", e)
            time.sleep(sleepSeconds)
            
        logging.info("service stopped")
        """

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("console", "debug"):
        svc = myDaemon(None)
        svc.stop_event = win32event.CreateEvent(None, 0, 0, None)
        svc.runDaemon()
    else:
        win32serviceutil.HandleCommandLine(myDaemon)