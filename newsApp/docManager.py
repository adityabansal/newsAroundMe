import os
import json
import calendar
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from cachingHelper import getCache
from constants import LINKTAG_PUBTIME, FEEDTAG_DO_NOT_CLUSTER
from dbhelper import parseConnectionString, getS3Connection
from doc import Doc

def _getEpochSecs(t):
    return calendar.timegm(time.strptime(t[:19], "%Y-%m-%dT%H:%M:%S"))

class DocManager:
    """
    Manage documents stored in cloud.

    Contains functions for CRUD operations on documents
    """

    def __init__(self):
        """
        Instantiates a new instance of DocManager class

        'bucketConnString' : connection string of s3 bucket in which docs
        are stored.
        """

        self.bucketConnString = os.environ['DOCSBUCKET_CONNECTIONSTRING']
        self.cache = getCache()
        self.__cacheExpiry= 900

    def __getBucket(self):
        bucketConnParams = parseConnectionString(self.bucketConnString)
        conn = getS3Connection(self.bucketConnString)

        return conn.get_bucket(bucketConnParams['bucketName'], validate=False)

    def __isDocNew(self, key, timeLimit):
        if _getEpochSecs(key.last_modified) < timeLimit:
            return False

        doc = self.get(key.name)
        return (doc.tags[LINKTAG_PUBTIME] > timeLimit) and \
            (FEEDTAG_DO_NOT_CLUSTER not in doc.tags)

    def put(self, doc):
        k = Key(self.__getBucket())
        k.key = doc.key

        # not storing tags directly in blob's metadata as the maximum size
        # allowed there is only 2kb.
        tags = dict(doc.tags)
        tags['content'] = doc.content
        keyContents = json.dumps(tags)
        k.set_contents_from_string(keyContents)
        self.cache.set(k.key, keyContents, self.__cacheExpiry)

    def get(self, docKey):
        keyContents = self.cache.get(docKey)
        if not keyContents:
            k = Key(self.__getBucket())
            k.key = docKey
            keyContents = k.get_contents_as_string()
            self.cache.set(docKey, keyContents, self.__cacheExpiry)

        storedTags = json.loads(keyContents)
        content = storedTags.pop('content', None)
        tags = storedTags

        return Doc(docKey, content, tags)

    def delete(self, docKey):
        k = Key(self.__getBucket())
        k.key = docKey
        k.delete()
        self.cache.delete(docKey)
