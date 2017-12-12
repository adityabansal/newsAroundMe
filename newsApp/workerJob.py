import random
import json

def _generateRandomJobId():
    return ''.join(random.choice('0123456789ABCDEF') for i in range(32));

class WorkerJob:
    """
    Represents a job which can be processed by one of the worker roles.
    """

    def __init__(self, jobName, jobParams, jobId = None):
        """
        Instantiates a new worker job object.
        Requires 'jobName': a string representing name of the job
        Requires 'jobParams': a dictionary where keys represnt job parameter
            names, and corresponding values the job parameter values.
        Optional 'jobId': a identier for this job. If not provided a
            randomly generated alphanumeric string is used.
        """

        if (jobId is None):
            self.jobId = _generateRandomJobId()
        else :
            self.jobId = jobId

        self.jobName = jobName
        self.jobParams = jobParams

    def deserializeFromString(self, serializedJob):
        """
        Sets this worker job object to the specified serialized string
        representation.
        """

        tempDict = json.loads(serializedJob)
        jobName = tempDict.pop('jobName', None)
        jobId = tempDict.pop('jobId', None)
        self.__init__(jobName, tempDict, jobId)

    def serializeToString(self):
        """
        Serialize to a human-readable string representation of this object
        """

        tempDict = dict(self.jobParams)
        tempDict['jobName'] = self.jobName
        tempDict['jobId'] = self.jobId
        return json.dumps(tempDict)
