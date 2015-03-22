import json
import logging
import random
import urllib

from constants import *
from doc import Doc
from docManager import DocManager
from link import Link
from linkManager import LinkManager
import htmlProcessor as hp
from publisher import Publisher
from publisherManager import PublisherManager

logger = logging.getLogger('linkProcessor')

def _getHtmlForUrl(url):
    sock = urllib.urlopen(url);
    html = sock.read();
    sock.close();

    return html;

def _generateRandomDocKey():
    return ''.join(random.choice('0123456789ABCDEF') for i in range(16));

def processLink(linkId):
  """
  Processes a link(takes as input the linkId)

  Steps:
  1. get link from database
  2. get publisher for that link from database
  3. get html for that link
  4. process that html to generate doc
  5. save that doc in docstore.
  6. update the link's is processed tag.
  """

  logger.info("Started processing link. Link id: %s.", linkId)
  
  # get the link
  linkManager = LinkManager();
  link = linkManager.get(linkId);
  logger.info("Got link from database. Link id: %s.", linkId)

  # get the publisher
  publisherManager = PublisherManager();
  publisher = publisherManager.get(link.tags[TAG_PUBLISHER]);
  logger.info(
    "Got publisher from database. Publisher id: %s. Link id: %s",
    link.tags[TAG_PUBLISHER],
    linkId)

  # get html for the link
  pageHtml = _getHtmlForUrl(link.id);
  logger.info("Got html for the link. Link id: %s.", linkId)

  # process that html
  linkManager = LinkManager()
  processingResult = hp.processHtml(
      pageHtml,
      publisher.tags[PUBLISHERTAG_TEXTSELECTOR],
      json.loads(publisher.tags[PUBLISHERTAG_IMAGESELECTORS]));

  # generate corresponding doc
  doc = Doc(_generateRandomDocKey(), processingResult[0], link.tags);
  doc.tags[TAG_IMAGES] = json.dumps(processingResult[1]);
  doc.tags[DOCTAG_URL] = linkId;

  # save the doc
  docManager = DocManager();
  docManager.put(doc);
  logger.info("Document generated and saved for link. Link id: %s.", linkId)

  # update the link
  link.tags[LINKTAG_ISPROCESSED] = 'true';
  link.tags[LINKTAG_DOCKEY] = doc.key;
  linkManager.put(link);
  logger.info(
    "Link updated after being successfully processed. Link id: %s.",
    linkId)

  logger.info("Completed processing link. Link id: %s.", linkId)
