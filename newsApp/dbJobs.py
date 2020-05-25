import logging

from .constants import *
from .dbhelper import *

logger = logging.getLogger('dbJobs')

def updateDbThroughput(
        jobId,
        connectionString,
        readThroughput,
        writeThroughput,
        indexName = None):

    jobInfo = "Job id: " + jobId;
    logger.info("Started updating table throughput. %s", jobInfo);

    table = getDbTable(connectionString)

    if indexName is None:
        table.update(throughput={
            'read': readThroughput,
            'write': writeThroughput,
        })
        logger.info("Updated table throughput. %s", jobInfo)
    else:
        table.update_global_secondary_index(global_indexes={
            indexName: {
                'read': readThroughput,
                 'write': writeThroughput,
            }
        })
        logger.info("Updated table's global index throughput. %s", jobInfo)

    logger.info("Completed updating table throughput. %s", jobInfo);


