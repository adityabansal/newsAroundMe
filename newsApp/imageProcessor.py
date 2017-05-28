import logging
import os
import random
import urllib2
import cStringIO

from PIL import Image
from boto.s3.key import Key
from boto.dynamodb2.fields import HashKey
from boto.exception import S3ResponseError

from cachingHelper import getCache
from dbhelper import *

logger = logging.getLogger('imageProcessor')

class ImageProcessor:
  """
  Process an image discovered on a news site.
  Processing steps:
  1. Ensure image is not blacklisted.
  2. Dowload the image.
  3. Downsize it  if necessary
  4. Save to S3 bucket
  """

  def __init__(self):
  	self.tableConnString = os.environ['BLACKLISTEDIMAGESTABLE_CONNECTIONSTRING'];
  	self.bucketConnString = os.environ['IMAGESBUCKET_CONNECTIONSTRING'];

  def __getTable(self):
    return getDbTable(self.tableConnString);

  def __getBucket(self):
    conn = getS3Connection(self.bucketConnString);
    bucketConnParams = parseConnectionString(self.bucketConnString);
    return conn.get_bucket(bucketConnParams['bucketName']);

  def __setupTable(self):
    logger.info("Setting up the images table")

    # delete existing table if it exists
    try:
      logger.info("Deleting the table ...")
      self.__getTable().delete();
      logger.info("Deleted the table. Waiting for sometime before creating a new one")
      time.sleep(20)
    except:
      logger.info("Delete table call failed. Maybe it didn't exist.")
      pass;# do nothing. Maybe there was no existing table

    # create new table
    logger.info("Creating the new table ...")
    tableConnectionParams = parseConnectionString(self.tableConnString);
    return Table.create(
      tableConnectionParams['name'],
      schema = [HashKey('url')],
      throughput = {
          'read': 1,
          'write': 1,
      }, connection = getDbConnection(tableConnectionParams))
    logger.info("Created the table")

  def __setupBucket(self):
    connectionParams = parseConnectionString(self.bucketConnString)
    conn = getS3Connection(self.bucketConnString);
    bucketName = connectionParams['bucketName']

    if conn.lookup(bucketName):
      logger.info("Bucket already exists. Deleting it")
      conn.delete_bucket(bucketName)

    logger.info("Creating a new bucket ...")
    conn.create_bucket(bucketName)
    logger.info("Created the bucket")

  def setup(self):
    # create new table
    self.__setupTable();

    #create new bucket
    self.__setupBucket();

  def __isImageBlackListed(self, imageUrl):
    table = self.__getTable();
    return (table.query_count(url__eq = imageUrl) != 0)

  def getImageContent(self, imageKey):
    try:
      k = Key(self.__getBucket())
      k.key = imageKey;
      keyContents = k.get_contents_as_string()
      return keyContents;
    except S3ResponseError:
      return None;

  def processImage(self, jobId, imageUrl):
    jobIdLog = "JobId: " + jobId;

    if self.__isImageBlackListed(imageUrl):
      logger.info(
        "Image url %s is blacklisted. %s. Not processing it.",
        imageUrl,
        jobIdLog)
      return;

    try:
      #Retrieve our source image from a URL
      imageRaw = urllib2.urlopen(imageUrl)
      logger.info("Feteched image using url. %s.", jobIdLog);
    except urllib2.HTTPError:
      logger.info("Could not fetch the image url %s. %s", imageUrl, jobIdLog)
      return;

    try:
      #Load the URL data into an image
      imageIO = cStringIO.StringIO(imageRaw.read())
      image = Image.open(imageIO)

      #Resize the image
      size = 200, 140
      image.thumbnail(size, Image.ANTIALIAS)
      logger.info("Image successfully resized. %s", jobIdLog)

      #NOTE, we're saving the image into a cStringIO object to avoid writing to disk
      outImage = cStringIO.StringIO()
      #You MUST specify the file type because there is no file name to discern it from
      try:
        image.save(outImage, 'JPEG')
      except IOError:
        image.convert('RGB').save(outImage, 'JPEG')

      logger.info("Saving the image in the bucket %s", jobIdLog)
      imageKey = ''.join(random.choice('0123456789ABCDEF') for i in range(16)) + ".jpg"
      bucket = self.__getBucket();
      k = bucket.new_key(imageKey);
      k.set_contents_from_string(outImage.getvalue())
      logger.info("Saved image in the bucket with key %s. %s", imageKey, jobIdLog)
      return imageKey;
    except Exception, e:
      logger.exception("Could not process image");
      pass;