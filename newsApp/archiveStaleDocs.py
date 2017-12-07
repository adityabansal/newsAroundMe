from constants import *
from clusterManager import ClusterManager
from loggingHelper import *
from minerJobManager import MinerJobManager
from workerJob import WorkerJob

InitLogging()

def archiveStaleDocs():
  """
  Remove the docs fro current working set
  Run this job periodically.
  """

  clusterManager = ClusterManager()
  jobManager = MinerJobManager()

  logging.info("Archiving old clusters.")
  staleClusters = clusterManager.archiveOldClusters()

  for cluster in staleClusters:
    for docKey in cluster:
      job = WorkerJob(JOB_CLEANUPDOC, { JOBARG_CLEANUPDOC_DOCID : docKey})
      jobManager.enqueueJob(job)
      logging.info(
        "Put cleanup doc job for docId: %s. Job id: %s",
        docKey,
        job.jobId)

  logging.info("Archived old clusters and cleaned up docs in them from working set.")

if __name__ == '__main__':
    archiveStaleDocs()
