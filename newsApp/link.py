from dbItem import DbItem
import urllib2

def getIdentifierUrl(url):
  """
  Returns a unique identifier for the link.
  Someimes 2 different links can actually point to the same web page
  due to redirects etc.
  """

  result = urllib2.urlopen(url)
  return result.geturl()

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

    DbItem.__init__(self, getIdentifierUrl(id), tags);
