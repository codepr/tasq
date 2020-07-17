"""
tasq.job.py
~~~~~~~~~~~
Jobs related classes and functions, provides abstractions over the concept of a
task.
"""
import sys
import uuid
import time
import enum
import dataclasses
from typing import Any, Optional


@enum.unique
class JobStatus(enum.IntEnum):
    OK = 0
    FAILED = 1
    PENDING = 2


class Job:

    """Simple class modeling a Job, wrapping function calls with arguments and
    giving it an ID

    Attributes
    ----------
    :type job_id: str
    :param job_id: The unique universal identifier of the job

    :type func: function
    :param func: The core of the task itself, the associated with the Task
                 that must be executed, it must be a callable

    """

    def __init__(self, job_id, func, *args, **kwargs):
        # Assign a default uuid in case of empty name
        self._job_id = job_id or uuid.uuid4()
        # Function to be executed, define the job purpose
        self._func = func
        # Positional arguments
        self._args = args
        # Keywords arguments
        self._delay = int(kwargs.pop("delay", 0))
        self._kwargs = kwargs
        # Start time of the job placeholder
        self._start_time = None
        # End time of the job
        self._end_time = None
        # Job status
        self._status = JobStatus.PENDING
        super().__init__()

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
        return self._status == JobStatus.OK

    def execution_time(self):
        """Return the time passed between the start of the execution and the
        end of it
        """
        try:
            return self._end_time - self._start_time
        except TypeError:
            return 0

    def add_delay(self, delay):
        """Add a delay to the function"""
        self._delay = delay

    def execute(self):
        """Execute the function with arguments and keyword arguments, if a
        given delay is specified, this method await till the timeout expires
        and then executes the job
        """
        if self._delay:
            time.sleep(self._delay)
        return self._execute_job()

    def _execute_job(self):
        """Execute the function with arguments and keyword arguments"""
        exc = None
        outcome = JobStatus.FAILED
        self._start_time = time.time()
        try:
            result = self._func(*self._args, **self._kwargs)
            outcome = JobStatus.OK
        except:  # noqa pylint: disable=bare-except
            # Failing
            self._status = JobStatus.FAILED
            result = None
            exc = sys.exc_info()[0]
        finally:
            self._end_time = time.time()
        return JobResult(
            self.job_id, outcome, result, exc, self.execution_time()
        )

    def __repr__(self):
        args = ", ".join(str(i) for i in self.args)
        kwargs = ", ".join(
            key + "=" + repr(self.kwargs[key]) for key in self.kwargs
        )
        arguments = f"({', '.join([args])}{', '.join([kwargs])})"
        arguments = (
            (arguments[:80] + "...)") if len(arguments) > 80 else arguments
        )
        if self.delay:
            arguments += f" delay: {self.delay}"
        return f"job ID: {self.job_id} - {self.func.__name__}{arguments}"

    @staticmethod
    def create(func, *args, **kwargs):
        name = kwargs.pop("name", None)
        return Job(job_id=name, func=func, *args, **kwargs)


@dataclasses.dataclass
class JobResult:

    """Wrapper class for results of task executions"""

    name: str
    outcome: JobStatus
    value: Optional[Any] = None
    exc: Optional[Exception] = None
    exec_time: Optional[float] = 0.0
