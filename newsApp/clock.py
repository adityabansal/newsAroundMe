import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from pushFeedJobs import pushFeedJobs
from pushLinkJobs import pushLinkJobs
from startClustering import startIncrementalClustering
from pushClusterJob import pushClusterJob
from cleanupStaleLinks import cleanupStaleLinks
from cleanupStaleDocs import cleanupStaleDocs

now = datetime.datetime.now()
clusteringInterval = 10

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes = 2, start_date = now)
def pushFeedJobs_job():
    pushFeedJobs()

@sched.scheduled_job(
	'interval',
	minutes =clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 3))
def startClustering_job():
	startIncrementalClustering()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 5))
def pushClusterJob_job():
	pushClusterJob()

@sched.scheduled_job('interval', minutes = 30, start_date = now)
def pushLinkJobs_job():
    pushLinkJobs()

@sched.scheduled_job('interval', minutes = 60, start_date = now)
def cleanupStaleLinks_job():
    cleanupStaleLinks()

@sched.scheduled_job('interval', minutes = 60, start_date = now)
def cleanupStaleDocs_job():
    cleanupStaleDocs()

sched.start()