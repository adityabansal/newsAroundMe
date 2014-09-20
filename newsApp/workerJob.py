class WorkerJob:
    """
    Represents a job which can be processed by one of the worker roles.
    """

    def __init__(self, jobName, jobParams):
        """
        Instantiates a new worker job object.
        Requires 'jobName': a string representing name of the job
        Requires 'jobParams': a dictionary where keys represnt job parameter
                              names, and corresponding values the job parameter values.
        """

        self.jobName = jobName
        self.jobParams = jobParams

    def deserializeFromString(self, serializedJob):
        """
        Sets this worker job object to the specified serialized string representation.
        """

        tempDict = eval(serializedJob)
        jobName = tempDict['jobName']
        tempDict.pop('jobName', None)
        self.__init__(jobName, tempDict)

    def serializeToString(self):
        """
        Serialize to a human-readable string representation of this object
        """

        tempDict = dict(self.jobParams)
        tempDict['jobName'] = self.jobName
        return str(tempDict)
