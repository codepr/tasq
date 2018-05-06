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
        # Function to be executed, define the job purpose
        self._func = func
        # Positional arguments
        self._args = args
        # Keywords arguments
        self._delay = int(kwargs.pop('delay', 0))
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

    @property
    def delay(self):
        return self._delay

    def finished_ok(self):
        if self._status == JobStatus.OK:
            return True
        return False

    def execution_time(self):
        """Return the time passed between the start of the execution and the end of it"""
        try:
            return self._end_time - self._start_time
        except TypeError:
            return 0

    def add_delay(self, delay):
        """Add a delay to the function"""
        self._delay = delay

    def execute(self):
        """Execute the function with arguments and keyword arguments, if a given delay is specified,
        this method await till the timeout expires and then executes the job"""
        if self._delay:
            time.sleep(self._delay)
            return self._execute_job()
        return self._execute_job()

    def _execute_job(self):
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

    def __repr__(self):
        args = ','.join(str(i) for i in self.args)
        kwargs = ','.join(key + '=' + repr(self.kwargs[key]) for key in self.kwargs)
        arguments = '(' + args
        if kwargs:
            arguments += ', ' + kwargs
        arguments += ')'
        if self.delay:
            arguments += f' delay: {self.delay}'
        return f"job ID: {self.job_id} - {self.func.__name__}{arguments}"


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
