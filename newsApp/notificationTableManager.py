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
    for docId in cluster:
      if self.isDocNotified(docId, notifier):
        return True;

    return False;

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