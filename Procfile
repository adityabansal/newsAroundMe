worker: python ./newsApp/worker.py
web: gunicorn newsApp.webRole:app --worker-class gevent
