import time
import unittest
from tasq.job import Job, JobStatus, JobResult


class TestJob(unittest.TestCase):
    def test_job_init(self):
        job = Job("job-1", lambda x: x + 1, 10)
        self.assertEqual(job.job_id, "job-1")
        self.assertEqual(job.status, JobStatus.PENDING)
        self.assertEqual(job.delay, 0)

    def test_job_create(self):
        job = Job.create(lambda x: x + 1, name="job-1", x=10)
        self.assertEqual(job.job_id, "job-1")
        self.assertEqual(job.status, JobStatus.PENDING)
        self.assertEqual(job.delay, 0)

    def test_job_execute(self):
        job = Job("job-1", lambda x: x + 1, 10)
        self.assertEqual(job.job_id, "job-1")
        self.assertEqual(job.status, JobStatus.PENDING)
        self.assertEqual(job.delay, 0)
        result = job.execute()
        self.assertTrue(isinstance(result, JobResult))
        self.assertEqual(result.value, 11)

    def test_job_set_delay(self):
        job = Job("job-1", lambda x: x + 1, 10)
        self.assertEqual(job.job_id, "job-1")
        self.assertEqual(job.status, JobStatus.PENDING)
        self.assertEqual(job.delay, 0)
        job.set_delay(1)
        t1 = time.time()
        result = job.execute()
        t2 = time.time()
        self.assertTrue(isinstance(result, JobResult))
        self.assertEqual(result.value, 11)
        self.assertAlmostEqual(t2 - t1, 1, delta=0.1)
