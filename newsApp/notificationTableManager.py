import os
import time
import json
from enum import Enum

from boto.dynamodb2.table import Table

from cluster import Cluster
from dbhelper import *
from constants import *

class Notifier(Enum):
  twitter = 1
  facebook = 2
  webPush = 3

class NotificationTableManager:
  def __init__(self):
    self.tableConnString = os.environ['NOTIFICATIONTABLE_CONNECTIONSTRING'];
    self.__table = None

  def __getTable(self):
    if not self.__table:
      self.__table = getDbTable(self.tableConnString);

    return self.__table;

  def __getNotificationStateForDoc(self, docId):
    table = self.__getTable();
    queryResult = list(table.query_2(docId__eq = docId))

    if queryResult:
      return queryResult[0]
    else:
      return None

  def isDocNotified(self, docId, notifier):
    notificationState = self.__getNotificationStateForDoc(docId);

    if not notificationState:
      return False;
    else:
      return notificationState.get(notifier.name, False)

  def isClusterNotified(self, cluster, notifier):
    nUnnotifiedDocs = 0
    for docId in cluster:
      if not self.isDocNotified(docId, notifier):
        nUnnotifiedDocs = nUnnotifiedDocs + 1;

    if nUnnotifiedDocs == len(cluster):
      # all docs in clusters are unNotified
      return False;
    elif nUnnotifiedDocs > 2:
      # at least 3 docs in the cluster are unNotified
      return False;
    else:
      # some notified docs and less than 3 unNotified docs
      return True;

  def setClusterNotified(self, cluster, notifier):
    table = self.__getTable();
    with table.batch_write() as batch:
      for docId in cluster:
        currentNotificationState = self.__getNotificationStateForDoc(docId)
        if not currentNotificationState:
          batch.put_item(data={
            'docId': docId,
            notifier.name: True
          })
        else:
          currentNotificationState[notifier.name] = True
          batch.put_item(data=currentNotificationState, overwrite = True)