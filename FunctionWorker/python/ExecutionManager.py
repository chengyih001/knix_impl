import os
import sys
import json
import time
import socket
import logging

from LocalQueueClient import LocalQueueClient, LocalQueueMessage
from LocalQueueClientMessage import LocalQueueClientMessage
from MicroFunctionsLogWriter import MicroFunctionsLogWriter

import py3utils

LOGGER_HOSTNAME = 'hostname-unset'
LOGGER_CONTAINERNAME = 'containername-unset'
LOGGER_UUID = '0l'
LOGGER_USERID = 'userid-unset'
LOGGER_WORKFLOWNAME = 'workflow-name-unset'
LOGGER_WORKFLOWID = 'workflow-id-unset'


class LoggingFilter(logging.Filter):
    def filter(self, record):
        global LOGGER_HOSTNAME
        global LOGGER_CONTAINERNAME
        global LOGGER_UUID
        global LOGGER_USERID
        global LOGGER_WORKFLOWNAME
        global LOGGER_WORKFLOWID
        record.timestamp = time.time()*1000000
        record.hostname = LOGGER_HOSTNAME
        record.containername = LOGGER_CONTAINERNAME
        record.uuid = LOGGER_UUID
        record.userid = LOGGER_USERID
        record.workflowname = LOGGER_WORKFLOWNAME
        record.workflowid = LOGGER_WORKFLOWID
        return True


class ExecutionManager:
    def __init__(self, args_dict):
        self._POLL_MAX_NUM_MESSAGES = 500
        self._POLL_TIMEOUT = py3utils.ensure_long(10000)

        self._set_args(args_dict)

        self._execution_map = {}    # execution_id -> {functionTopic -> nextTopic}

        self._parent_workers = {}   # functionTopic -> FunctionWorker instance
        self._available_workers = {}    # functionTopic -> [pid]
        self._busy_workers = {}     # functionTopic -> [pid]
        
        self._local_queue_client = LocalQueueClient(connect=self._queue)

        self._setup_loggers()

    def _setup_loggers(self):
        global LOGGER_HOSTNAME
        global LOGGER_CONTAINERNAME
        global LOGGER_USERID
        global LOGGER_WORKFLOWNAME
        global LOGGER_WORKFLOWID

        LOGGER_HOSTNAME = self._hostname
        LOGGER_CONTAINERNAME = socket.gethostname()
        LOGGER_USERID = self._userid
        LOGGER_WORKFLOWNAME = self._workflowname
        LOGGER_WORKFLOWID = self._workflowid

        self._logger = logging.getLogger("ExecutionManager")
        self._logger.setLevel(logging.INFO)
        self._logger.addFilter(LoggingFilter())

        formatter = logging.Formatter(
            "[%(timestamp)d] [%(levelname)s] [%(hostname)s] [%(containername)s] [%(uuid)s] [%(userid)s] [%(workflowname)s] [%(workflowid)s] [%(name)s] [%(asctime)s.%(msecs)03d] %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        logfile = '/opt/mfn/logs/ExecutionManager.log'

        hdlr = logging.FileHandler(logfile)
        hdlr.setLevel(logging.INFO)
        hdlr.setFormatter(formatter)
        self._logger.addHandler(hdlr)

        global print
        print = self._logger.info
        sys.stdout = MicroFunctionsLogWriter(self._logger, logging.INFO)
        sys.stderr = MicroFunctionsLogWriter(self._logger, logging.ERROR)

    def _set_args(self, args):
        self._userid = args["userid"]
        self._storage_userid = args["storageuserid"]
        self._sandboxid = args["sandboxid"]
        self._workflowid = args["workflowid"]
        self._workflowname = args["workflowname"]
        self._hostname = args["hostname"]
        self._queue = args["queue"]
        self._datalayer = args["datalayer"]
        self._external_endpoint = args["externalendpoint"]
        self._internal_endpoint = args["internalendpoint"]
        self._management_endpoints = args["managementendpoints"]

        # _XXX_: also includes the workflow end point (even though it is not an actual function)
        self._wf_function_list = args["workflowfunctionlist"]
        self._wf_exit = args["workflowexit"]

        self._is_session_workflow = False
        if args["sessionworkflow"]:
            self._is_session_workflow = True

        self._is_session_function = False
        if args["sessionfunction"]:
            self._is_session_function = True
        self._session_function_parameters = args["sessionfunctionparameters"]
        self._usertoken = os.environ["USERTOKEN"]

        self._should_checkpoint = args["shouldcheckpoint"]

    def _get_and_handle_message(self):
        # pull topic ExecutionManager (a new FunctionWorker instance is reporting)
        lqm = self._local_queue_client.getMessage("executionManager", self._POLL_TIMEOUT)
        if lqm is not None:
            self._handle_message(lqm)

    def _handle_message(self, lqm):
        # handle message
        try:
            lqcm = LocalQueueClientMessage(lqm=lqm)
            key = lqcm.get_key()
            value = lqcm.get_value()
            if key == "new_fw_reporting":
                self._handle_reporting_worker(worker_detail=value)
        except Exception as exc:
            self._logger.exception("Exception in handling: %s", str(exc))
            sys.stdout.flush()

    def _handle_reporting_worker(self, worker_detail):
        # append pid of FunctionWorker instance to dict
        info = json.loads(worker_detail)
        functionTopic = info["functionTopic"]
        pid = info["pid"]
        if self._available_workers[functionTopic] is None:
            self._available_workers[functionTopic] = []
            self._available_workers[functionTopic].append(pid)
        else:
            self._available_workers[functionTopic].append(pid)

    def _update_execution_map(self, execution_instance):
        # update Execution-instance map
        pass

    def swapin(self, pid):
        # swap in process
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "w") as f:
            f.write(str(17179869184))
        with open("/sys/fs/cgroup/memory/{}/memory.swappiness".format(pid), "w") as f:
            f.write(str(60))

    def swapout(self, pid, min_resident_mem=262144):
        # swap out process
        mem = 1073741824
        while mem >= min_resident_mem:
            try:
                with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "w") as f:
                    f.write(str(mem))
                mem = int(mem / 2)
            except:
                break
        with open("/sys/fs/cgroup/memory/{}/memory.swappiness".format(pid), "w") as f:
            f.write(str(100))

    def add_worker(self, functionTopic):
        # create new FunctionWorker instance
        self._parent_workers[functionTopic]._fork()

    def allocate_worker(self, functionTopic):
        # swap in a FunctionWorker instance
        if len(self._available_workers[functionTopic]) > 0:
            for pid in self._available_workers[functionTopic]:
                self._available_workers[functionTopic].pop(pid)
                if self._busy_workers[functionTopic] is None:
                    self._busy_workers[functionTopic] = []
                    self._busy_workers[functionTopic].append(pid)
                else:
                    self._busy_workers[functionTopic].append(pid)
                self.swapin(pid)
                return pid
        else:
            # TODO
            # Setup pool policy to avoid creating a bunch of workers
            self.add_worker(functionTopic)
            self.allocate_worker(functionTopic)

    def free_worker(self, functionTopic, pid):
        # swap out a FunctionWorker instance
        if pid in self._busy_workers[functionTopic]:
            self._busy_workers[functionTopic].pop(pid)
            self._available_workers[functionTopic].append(pid)
            self.swapout(pid)

    def update_worker(self, functionTopic, pid, value):
        # update workers
        lqcm = LocalQueueClientMessage(key="0l", value=value)
        ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm, True)
        while not ack:
            ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm, True)

    def _loop(self):
        # TODO
        # Create Execution-instance Maps
        # Listen to executionManger topics
        pass

    def _exit(self):
        # TODO
        # add method to shutdown java process

        # shutdown all FunctionWorker instances
        shutdown_message = {}
        shutdown_message["action"] = "stop"
        lqcm_shutdown = LocalQueueClientMessage(key="0l", value=json.dumps(shutdown_message))
        
        for functionTopic in self._available_workers.keys():
            for pid in self._available_workers[functionTopic]:
                ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm_shutdown, True)
                while not ack:
                    ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm_shutdown, True)
                self._logger.info("Waiting for child function workers to shutdown")
        
        for parent in self._parent_workers.values():
            functionTopic = parent._function_topic
            pid = parent._pid
            ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm_shutdown, True)
            while not ack:
                ack = self._local_queue_client.addMessage("{}-{}".format(functionTopic, pid), lqcm_shutdown, True)
            self._logger.info("Waiting for parent function workers to shutdown")
        
        # shutdown local_queue
        self._local_queue_client.shutdown()

        # shutdown ExecutionManager
        self.running = False

    def run(self):
        self._running = True
        while self._running:
            self._loop()