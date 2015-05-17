from dbItem import DbItem
import time
import urllib2

def _openUrlWithRetries(url, max_retries = 3):
    nRetries = 0;
    while (True):
        try:
            response = urllib2.urlopen(url, timeout = 10);
            return response;
        except Exception as e:
            if (nRetries >= max_retries):
                raise e;
            else:
                time.sleep(10);
                nRetries = nRetries + 1;

def getIdentifierUrl(url):
  """
  Returns a unique identifier for the link.
  Someimes 2 different links can actually point to the same web page
  due to redirects etc.
  """

  result = _openUrlWithRetries(url);
  return result.geturl();

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

  def getHtml(self):
    response = _openUrlWithRetries(self.id);
    html = response.read();
    response.close();

    return html;
