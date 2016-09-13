import os
import json
import calendar
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from constants import *
from dbhelper import *
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

        self.bucketConnString = os.environ['DOCSBUCKET_CONNECTIONSTRING'];

    def __getBucket(self):
        bucketConnParams = parseConnectionString(self.bucketConnString);

        conn = S3Connection(
            bucketConnParams['accessKeyId'],
            bucketConnParams['secretAccessKey']);

        return conn.get_bucket(bucketConnParams['bucketName']);

    def __isDocNew(self, key, timeLimit):
        if _getEpochSecs(key.last_modified) < timeLimit:
            return False;

        doc = self.get(key.name);
        return doc.tags[LINKTAG_PUBTIME] > timeLimit

    def put(self, doc):
        k = Key(self.__getBucket());
        k.key = doc.key;

        # not storing tags directly in blob's metadata as the maximum size
        # allowed there is only 2kb.
        tags = dict(doc.tags);
        tags['content'] = doc.content;
        k.set_contents_from_string(json.dumps(tags))

    def getNewDocKeys(self, ageLimit):
        bucket = self.__getBucket();
        timeLimit = int(time.time()) - ageLimit * 60 * 60 * 24;

        return (key.name for key in bucket if self.__isDocNew(key, timeLimit))

    def get(self, docKey):
        k = Key(self.__getBucket());
        k.key = docKey;
        storedTags = json.loads(k.get_contents_as_string());
        content = storedTags.pop('content', None);
        tags = storedTags;

        return Doc(k.key, content, tags);

    def delete(self, docKey):
        k = Key(self.__getBucket());
        k.key = docKey;
        k.delete();
