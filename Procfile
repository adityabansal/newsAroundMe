minerWorker: python ./newsApp/worker.py -q MINER_JOBSQUEUE_CONNECTIONSTRING
clusterworker: python ./newsApp/worker.py -q CLUSTER_JOBSQUEUE_CONNECTIONSTRING
web: gunicorn newsApp.webRole:app --worker-class gevent
