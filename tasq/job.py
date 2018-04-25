# -*- coding: utf-8 -*-

"""
tasq.job.py
~~~~~~~~~
"""


class Job:

    """Simple class modeling a Job, wrapping function calls with arguments and giving it an ID"""

    def __init__(self, job_id, func, *args, **kwargs):
        self._job_id = job_id
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def execute(self):
        return self._func(*self._args, **self._kwargs)
