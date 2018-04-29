# -*- coding: utf-8 -*-

"""
tasq.job.py
~~~~~~~~~~~
Jobs related classes and functions
"""

import uuid


class Job:

    """Simple class modeling a Job, wrapping function calls with arguments and giving it an ID"""

    def __init__(self, job_id, func, *args, **kwargs):
        # Assign a default uuid in case of empty name
        self._job_id = job_id or uuid.uuid4()
        self._func = func
        self._args = args
        self._kwargs = kwargs

    @property
    def job_id(self):
        return self._job_id

    def execute(self):
        """Execute the function with arguments and keyword arguments"""
        return self._func(*self._args, **self._kwargs)


class JobResult:

    """Wrapper class for results of task executions"""

    def __init__(self, name, value):
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value
