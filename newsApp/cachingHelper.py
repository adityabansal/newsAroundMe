import os

import bmemcached

def getCache():
  servers = os.environ.get('MEMCACHIER_SERVERS', '').split(',')
  user = os.environ.get('MEMCACHIER_USERNAME', '')
  passw = os.environ.get('MEMCACHIER_PASSWORD', '')

  return bmemcached.Client(servers, user, passw)
