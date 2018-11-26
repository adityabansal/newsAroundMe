import os
import logging
from datetime import datetime
from pytz import timezone

logger = logging.getLogger('notifierTwitter')

class NotifierBase:
  def __init__(self):
    self.domainName = os.environ['DOMAIN']

  def isNightTime(self, locale):
    # for now we only have cities in india
    india_tz = timezone('Asia/Kolkata')
    hour = datetime.now(india_tz).hour
    return hour >= 2 and hour < 7