import logging
import os
import random
import requests
import time
import io

from PIL import Image
from boto.s3.key import Key
from boto.dynamodb2.fields import HashKey
from boto.exception import S3ResponseError

from .cachingHelper import getCache
from .dbhelper import *

logger = logging.getLogger('imageProcessor')

class ImageProcessor:
  """
  Process an image discovered on a news site.
  Processing steps:
  1. Ensure image is not blacklisted.
  2. Dowload the image.
  3. Downsize it if necessary
  4. Save to S3 bucket
  """

  def __init__(self):
    self.blacklistTableConnString = os.environ['BLACKLISTEDIMAGESTABLE_CONNECTIONSTRING']
    self.__blacklistTable = None
    self.mappingTableConnString = os.environ['IMAGEMAPPINGSTABLE_CONNECTIONSTRING']
    self.__mappingTable = None
    self.bucketConnString = os.environ['IMAGESBUCKET_CONNECTIONSTRING']

  def __getBlacklistTable(self):
    if not self.__blacklistTable:
      self.__blacklistTable = getDbTable(self.blacklistTableConnString)

    return self.__blacklistTable

  def __getMappingTable(self):
    if not self.__mappingTable:
      self.__mappingTable = getDbTable(self.mappingTableConnString)

    return self.__mappingTable

  def __getBucket(self):
    conn = getS3Connection(self.bucketConnString)
    bucketConnParams = parseConnectionString(self.bucketConnString)
    return conn.get_bucket(bucketConnParams['bucketName'])

  def __setupTable(self):
    logger.info("Setting up the images table")

    # delete existing table if it exists
    try:
      logger.info("Deleting the table ...")
      self.__getMappingTable().delete()
      logger.info("Deleted the table. Waiting for sometime before creating a new one")
      time.sleep(20)
    except:
      logger.info("Delete table call failed. Maybe it didn't exist.")
      pass # do nothing. Maybe there was no existing table

    # create new table
    logger.info("Creating the new table ...")
    tableConnectionParams = parseConnectionString(self.blacklistTableConnString)
    return Table.create(
      tableConnectionParams['name'],
      schema = [HashKey('url')],
      throughput = {
          'read': 1,
          'write': 1,
      }, connection = getDbConnection(tableConnectionParams))

  def __setupBucket(self):
    connectionParams = parseConnectionString(self.bucketConnString)
    conn = getS3Connection(self.bucketConnString)
    bucketName = connectionParams['bucketName']

    if conn.lookup(bucketName):
      logger.info("Bucket already exists. Deleting it")
      conn.delete_bucket(bucketName)

    logger.info("Creating a new bucket ...")
    conn.create_bucket(bucketName)
    logger.info("Created the bucket")

  def setup(self):
    # create new table
    self.__setupTable()

    #create new bucket
    self.__setupBucket()

  def __isImageBlackListed(self, imageUrl):
    table = self.__getBlacklistTable()
    return (table.query_count(url__eq = imageUrl) != 0)

  def __getImageMapping(self, imageUrl):
    table = self.__getMappingTable()
    mappings = list(table.query(url__eq = imageUrl))

    if not mappings:
      return
    else:
      return mappings[0]['imageKey']

  def __addImageMapping(self, imageUrl, imageKey):
    self.__getMappingTable().put_item(data={
      'url': imageUrl,
      'imageKey': imageKey
    })

  def getImageContent(self, imageKey):
    try:
      k = Key(self.__getBucket())
      k.key = imageKey
      keyContents = k.get_contents_as_string()
      return keyContents
    except S3ResponseError:
      return None

  def processImage(self, jobId, imageUrl):
    jobIdLog = "JobId: " + jobId
    logger.info(
        "Started processing image with url %s. %s",
        imageUrl,
        jobIdLog)

    if imageUrl.startswith("data:image/"):
      logger.info("Not processing image with data URI scheme %s", jobIdLog)
      return

    if self.__isImageBlackListed(imageUrl):
      logger.info(
        "Image url %s is blacklisted. %s. Not processing it.",
        imageUrl,
        jobIdLog)
      return

    imageMapping = self.__getImageMapping(imageUrl)
    if imageMapping:
      logger.info(
        "Image url %s has already been processed. %s. Not reprocessing it.",
        imageUrl,
        jobIdLog)
      return imageMapping

    try:
      #Retrieve our source image from a URL
      r = requests.get(imageUrl, stream=True, timeout = 10)
      r.raw.decode_content = True
      imageRaw = r.raw
      logger.info("Feteched image using url. %s.", jobIdLog)
    except Exception:
      logger.info("Could not fetch the image url %s. %s", imageUrl, jobIdLog)
      return

    try:
      image = Image.open(imageRaw)

      #See if the image is too small
      width, height = image.size
      if width < 150 or height < 150:
        logger.info(
          "Image size is too small. Not processing it. Size %ix%i. %s",
          width,
          height,
          jobIdLog)
        return

      # crop image if its too long
      if height > width:
        image = image.crop((0, 0, width, width))
        logger.info("Image too long. Cropped bottom part. %s", jobIdLog)

      # crop image if its too wide
      if width > 2*height:
        image = image.crop((0, 0, 2*height, height))
        logger.info("Image too wide. Cropped right part. %s", jobIdLog)

      #Resize the image
      newSize = 350, 350
      image.thumbnail(newSize, Image.ANTIALIAS)
      logger.info("Image successfully resized. %s", jobIdLog)

      #NOTE, we're saving the image into a BytesIO object to avoid writing to disk
      outImage = io.BytesIO()
      #You MUST specify the file type because there is no file name to discern it from
      try:
        image.save(outImage, 'JPEG')
      except IOError:
        image.convert('RGB').save(outImage, 'JPEG')

      logger.info("Saving the image in the bucket %s", jobIdLog)
      imageKey = ''.join(random.choice('0123456789ABCDEF') for i in range(16)) + ".jpg"
      bucket = self.__getBucket()
      k = bucket.new_key(imageKey)
      k.set_contents_from_string(outImage.getvalue())
      logger.info("Saved image in the bucket with key %s. %s", imageKey, jobIdLog)

      self.__addImageMapping(imageUrl, imageKey)
      logger.info("Added entry in image mappings table")

      return imageKey
    except IOError:
      logger.info(
        "Encountered IO error in processing image with url %s. %s",
        imageUrl,
        jobIdLog)
    except Exception:
      logger.exception(
        "Could not process image with url %s. %s",
        imageUrl,
        jobIdLog)
      pass