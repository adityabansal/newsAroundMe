import os
import json

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from dbhelper import *
from doc import Doc

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

    def put(self, doc):
        k = Key(self.__getBucket());
        k.key = doc.key;

        # not storing tags directly in blob's metadata as the maximum size
        # allowed there is only 2kb.
        tags = dict(doc.tags);
        tags['content'] = doc.content;
        k.set_contents_from_string(json.dumps(tags))

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
