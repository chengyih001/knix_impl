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
        self._instances = {}        # functionTopic -> [pid]
        
        self.local_queue_client = LocalQueueClient(connect=self._queue)

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
        # TODO
        # pull topic ExecutionManager (a new FunctionWorker instance is reporting)
        pass

    def _handle_message(self):
        # TODO
        # handle 
        pass

    def _handle_reporting_worker(self):
        # TODO
        # append pid of FunctionWorker instance to dict
        pass

    def _update_execution_map(self, execution_instance):
        # TODO
        # update Execution-instance map
        pass

    def add_worker(self):
        # TODO
        # create new FunctionWorker instance
        pass

    def allocate_worker(self, pid):
        # TODO
        # swap in a FunctionWorker instance
        pass

    def free_worker(self, pid):
        # TODO
        # swap out a FunctionWorker instance
        pass

    def update_worker(self, value):
        # TODO
        # update workers
        pass

    def _loop(self):
        pass

    def _exit(self):
        pass

    def run(self):
        pass