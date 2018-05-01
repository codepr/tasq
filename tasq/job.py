# -*- coding: utf-8 -*-

"""
tasq.job.py
~~~~~~~~~~~
Jobs related classes and functions
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import uuid
import time
from enum import IntEnum


class JobStatus(IntEnum):
    OK = 0
    FAILED = 1
    PENDING = 2


class Job:

    """Simple class modeling a Job, wrapping function calls with arguments and giving it an ID"""

    def __init__(self, job_id, func, *args, **kwargs):
        # Assign a default uuid in case of empty name
        self._job_id = job_id or uuid.uuid4()
        self._func = func
        self._args = args
        self._kwargs = kwargs
        # Start time of the job placeholder
        self._start_time = None
        # End time of the job
        self._end_time = None
        # Job status
        self._status = JobStatus.PENDING

    @property
    def job_id(self):
        return self._job_id

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def status(self):
        return self._status

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    def finished_ok(self):
        if self._status == JobStatus.OK:
            return True
        return False

    def execution_time(self):
        return self._end_time - self._start_time

    def execute(self):
        """Execute the function with arguments and keyword arguments"""
        self._start_time = time.time()
        try:
            result = JobResult(self.job_id, self._func(*self._args, **self._kwargs))
        except:
            # Failing
            self._status = JobStatus.FAILED
            result = JobResult(self.job_id, None, sys.exc_info()[0])
        finally:
            self._end_time = time.time()
        return result


class JobResult:

    """Wrapper class for results of task executions"""

    def __init__(self, name, value, exc=None):
        self._name = name
        self._value = value
        self._exc = exc

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def exc(self):
        return self._exc
