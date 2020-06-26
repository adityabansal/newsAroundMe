import os
import logging
from datetime import datetime
from pytz import timezone
import time

from boto.dynamodb2.table import Table
import tweepy

from .dbhelper import getDbTable, decryptSecret, encryptSecret
from .notifierBase import NotifierBase

logger = logging.getLogger('notifierTwitter')

class NotifierTwitter(NotifierBase):
  def __init__(self):
    NotifierBase.__init__(self)
    self.tableConnString = os.environ['TWITTERHANDLESTABLE_CONNECTIONSTRING']
    self.encryptionKey = os.environ['TWITTERHANDLESTABLE_KEY']
    self.__table = None

  def __getTable(self):
    if not self.__table:
      self.__table = getDbTable(self.tableConnString)

    return self.__table

  def __getKeys(self, handle):
    table = self.__getTable()
    dbRows = list(table.query_2(handle__eq = handle))

    if not dbRows:
      return None

    dbRow = dbRows[0]
    return {
      'handle': dbRow['handle'],
      'consumerKey': decryptSecret(dbRow['consumerKey'], self.encryptionKey),
      'consumerSecret': decryptSecret(dbRow['consumerSecret'], self.encryptionKey),
      'token': decryptSecret(dbRow['token'], self.encryptionKey),
      'tokenSecret': decryptSecret(dbRow['tokenSecret'], self.encryptionKey)
    }

  def __getTwitterApi(self, jobId, handle):
    keys = self.__getKeys(handle)
    logging.info("Got secrets for twitter handle %s. Job id: %s", handle, jobId)

    auth = tweepy.OAuthHandler(keys['consumerKey'], keys['consumerSecret'])
    auth.set_access_token(keys['token'], keys['tokenSecret'])
    return tweepy.API(auth)

  def addHandle(self, handle, consumerKey, consumerSecret, token, tokenSecret):
    table = self.__getTable()
    table.put_item(data={
      'handle': handle,
      'consumerKey': encryptSecret(consumerKey, self.encryptionKey),
      'consumerSecret': encryptSecret(consumerSecret, self.encryptionKey),
      'token': encryptSecret(token, self.encryptionKey),
      'tokenSecret': encryptSecret(tokenSecret, self.encryptionKey)      
    })

  def getNotificationText(self, cluster):
    storyUrl = "https://" + self.domainName + "/story/" + cluster.articles[0]['id']
    tweetText = ""
    linkLength = 23 #t.co length
    tweetLength = linkLength

    for article in cluster.articles:
      # don't tweet old articles in cluster
      if article['publishedOn'] > (int(time.time()) - 18 * 60 * 60):
        articleTitle = article['title']
        articleLink = article['link']
        articleText = articleTitle + " (via: " + articleLink + ")\n\n"
        #  "{} (via: {})\n".format(articleTitle, articleLink)
        articleTextLength = len(articleText) - (len(articleLink) - linkLength)
        if (tweetLength + articleTextLength) < 240:
          tweetText = tweetText + articleText
          tweetLength = tweetLength + articleTextLength
        else:
          break

    tweetText = tweetText + storyUrl
    return tweetText

  def doesLocaleExist(self, locale):
    if not self.__getKeys(locale):
      return False
    else:
      return True

  def notifyForLocales(self, jobId, cluster):
    jobLog  = "Job id: " + jobId

    tweetText = self.getNotificationText(cluster)
    logging.info("Going to tweet'%s'. %s", tweetText, jobLog)

    for locale in cluster.locales:
      if self.doesLocaleExist(locale):
        api = self.__getTwitterApi(jobId, locale)
        logging.info("Got the twitter api interface for %s. %s", locale, jobLog)

        api.update_status(tweetText)
        logging.info("Posted the tweet successfully. Job id: %s", jobLog)
