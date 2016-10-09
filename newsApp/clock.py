import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from pushFeedJobs import pushFeedJobs
from startClustering import startIncrementalClustering
from pushClusterJob import pushClusterJob

now = datetime.datetime.now()
clusteringInterval = 10

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes =8, start_date = now)
def pushFeedJobs_job():
    pushFeedJobs()

@sched.scheduled_job(
	'interval',
	minutes =clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 2))
def startClustering_job():
	startIncrementalClustering()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 4))
def pushClusterJob_job():
	pushClusterJob()

sched.start()