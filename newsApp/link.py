from .dbItem import DbItem
from .webPageLoader import loadPageAndGetHtml, getHtmlStatic
import time
import requests

def _openUrlWithRetries(url, max_retries = 1):
    nRetries = 0
    while (True):
        try:
            response = requests.get(url, timeout = 20)
            return response
        except Exception as e:
            if (nRetries >= max_retries):
                raise e
            else:
                time.sleep(5)
                nRetries = nRetries + 1

def getIdentifierUrl(url):
  """
  Returns a unique identifier for the link.
  Someimes 2 different links can actually point to the same web page
  due to redirects etc.
  """

  result = _openUrlWithRetries(url)
  return result.url

class Link(DbItem):
  """
  Represents a link to a webPage.

  Each wlink consists of a unique identifier(the url)
  and a set of tags(key-value pairs).
  """

  def __init__(self, id, tags=None):
    """
    Instantiates a link object representing a link to a web page.
    """

    DbItem.__init__(self, id, tags)

  def checkExistence(self):
    try:
      _openUrlWithRetries(self.id)
    except:
      return False

    return True

  def getFinalRedirect(self):
    return getIdentifierUrl(self.id)

  def getHtmlDynamic(self):
    return loadPageAndGetHtml(self.id)

  def getHtmlStatic(self):
    return getHtmlStatic(self.id)