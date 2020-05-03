import unittest
import time

from newsApp.workerJob import WorkerJob
from newsApp.jobManager import JobManager

class JobManagerTests(unittest.TestCase):

    def __compareJobs(self, job1, job2):
        self.assertTrue(job1.jobId, job2.jobId)
        self.assertTrue(job1.jobName, job2.jobName)
        self.assertTrue(job1.jobParams, job2.jobParams)

    def testEnqueueDequeueJobs(self):
        testJob1 = WorkerJob(
            'jobType1',
            { 'param1Name' : 'dummy1', 'param2Name' : 'dummy2' })
        testJob2 = WorkerJob(
            'jobType2',
            { 'param1Name' : 'dummy3', 'param2Name' : 'dummy4', 'param3Name' : 'dummy5' })
        testJob3 = WorkerJob(
            'jobType3',
            { 'param1Name' : 'dummy6' })

        # enqueue job1 and retrieve using dequeueJob
        testJobManager = JobManager('TEST_JOBSQUEUE_CONNECTIONSTRING')
        testJobManager.enqueueJob(testJob1)
        time.sleep(1)
        retrievedJob1 = testJobManager.dequeueJob()
        self.__compareJobs(testJob1, retrievedJob1)

        # enqueue rest of jobs
        testJobManager.enqueueJob(testJob2)
        testJobManager.enqueueJob(testJob3)
        testJobManager.enqueueJob(testJob3)

        # validate count()
        time.sleep(15)
        nJobs = testJobManager.count()
        self.assertEqual(nJobs, 3)

        # validate dequeueJobOfType()
        nonExistingJob = testJobManager.dequeueJobOfType(['nonExistingType'])
        self.assertIsNone(nonExistingJob)
        time.sleep(55)
        nJobs = testJobManager.count()
        self.assertEqual(nJobs, 3)

        retrievedJob3 = testJobManager.dequeueJobOfType(['jobType3'])
        self.__compareJobs(testJob3, retrievedJob3)
        time.sleep(55)
        nJobs = testJobManager.count()
        self.assertEqual(nJobs, 2)

        retrievedJob2 = testJobManager.dequeueJobOfType(['jobType2'])
        self.__compareJobs(testJob2, retrievedJob2)
        time.sleep(55)
        nJobs = testJobManager.count()
        self.assertEqual(nJobs, 1)

        retrievedJob3 = testJobManager.dequeueJob()
        self.__compareJobs(testJob3, retrievedJob3)

        # validate there is no job left in the queue
        self.assertTrue(testJobManager.dequeueJob() is None)
