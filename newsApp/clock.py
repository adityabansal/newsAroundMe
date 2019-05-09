import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from pushFeedJobs import pushFeedJobs
from pushLinkJobs import pushLinkJobs
from clusterDocsJob import clusterDocsJob
from archiveStaleDocs import archiveStaleDocs
from reprocessCurrentClusters import reprocessCurrentClusters
from notifyTwitter import notifyTwitter
from clusteringJobs import getCandidateDocsThroughClusters, cleanUpDistanceTable
from deepCleanStaleDocs import deepCleanStaleDocs

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
	minutes = clusteringInterval * 2,
	start_date = now + datetime.timedelta(minutes = 7))
def reprocessCurrentClusters_job():
    reprocessCurrentClusters()

@sched.scheduled_job(
	'interval',
	minutes = clusteringInterval,
	start_date = now + datetime.timedelta(minutes = 9))
def notifyTwitterJob_job():
	notifyTwitter()

@sched.scheduled_job(
	'interval',
	minutes = 60,
	start_date = now + datetime.timedelta(minutes = 10))
def cleanUpDistanceTable_job():
    cleanUpDistanceTable("")

@sched.scheduled_job(
	'interval',
	minutes = 60,
	start_date = now + datetime.timedelta(minutes = 15))
def getCandidateDocsThroughClusters_job():
    getCandidateDocsThroughClusters("")

@sched.scheduled_job(
	'interval',
	hours = 30,
	start_date = now + datetime.timedelta(hours = 2))
def deepCleanStaleDocs_job():
    deepCleanStaleDocs()

sched.start()