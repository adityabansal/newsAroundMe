import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from pushFeedJobs import pushFeedJobs
from pushLinkJobs import pushLinkJobs
from clusterDocsJob import clusterDocsJob
from archiveStaleDocs import archiveStaleDocs
from reprocessCurrentClusters import reprocessCurrentClusters

now = datetime.datetime.now()
clusteringInterval = 10

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes = 2, start_date = now)
def pushFeedJobs_job():
    pushFeedJobs()

@sched.scheduled_job('interval', minutes = 4, start_date = now)
def pushLinkJobs_job():
    pushLinkJobs()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 5))
def clusterDocsJob_job():
	clusterDocsJob()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 4))
def archiveStaleDocs_job():
    archiveStaleDocs()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 7))
def reprocessCurrentClusters_job():
    reprocessCurrentClusters()

sched.start()