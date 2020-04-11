import unittest

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

        # enqueue jobs
        testJobManager = JobManager('TEST_JOBSQUEUE_CONNECTIONSTRING')
        testJobManager.enqueueJob(testJob1)
        testJobManager.enqueueJob(testJob2)

        # dequeue jobs
        retrievedJob1 = testJobManager.dequeueJob()
        retrievedJob2 = testJobManager.dequeueJob()

        # validate jobs retrieved are same as jobs put
        self.__compareJobs(testJob1, retrievedJob1)
        self.__compareJobs(testJob2, retrievedJob2)

        # validate there is no job in the queue
        self.assertTrue(testJobManager.dequeueJob() is None)
