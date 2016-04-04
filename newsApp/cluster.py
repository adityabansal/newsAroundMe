import hashlib

from constants import *
from docManager import DocManager
from distanceTableManager import DistanceTableManager

DOC_DUPLICATION_THRESHOLD = 0.85
def _removeDuplicatesAndOutliers(items, articleCount):
  d = {}
  for item in items:
    if item in d:
      d[item] = d[item] + 1
    else:
      d[item] = 1

  return [item for item in d.keys() if d[item] > 0.3 * articleCount]

def _isDuplicateArticle(docKey, docsAdded):
  distanceTableManager = DistanceTableManager()

  for addedDoc in docsAdded:
    distance = distanceTableManager.getDistance(docKey, addedDoc)
    if distance > DOC_DUPLICATION_THRESHOLD:
      return True

  return False

class Cluster(set):
  """
  Represents a cluster(set of similar documents)
  """

  def __init__(self, docList):
    """
    Instantiates a new cluster object.
    Requires 'docList': a list of docIds comprising the cluster
    """

    set.__init__(self, docList)

    docList.sort();
    self.id = hashlib.md5(str(docList)).hexdigest().upper()

  def process(self):
    """
    Processes the cluster to include metadata of consisting documents,
    and overall metadata of cluster like category, location, feeds, etc.
    """

    self.categories = []
    self.countries = []
    self.locales = []
    self.publishers = []
    self.languages = []
    self.articles = [] # contains non-duplicate articles
    self.duplicates = []

    docManager = DocManager()
    docsAdded = []
    for docKey in super(Cluster, self).__iter__():
      doc = docManager.get(docKey)

      if not _isDuplicateArticle(docKey, docsAdded):
        self.articles.append({
          'title': doc.tags.get(LINKTAG_TITLE, ""),
          'publisher': doc.tags.get(TAG_PUBLISHER_DETAILS, ""),
          'link': doc.tags.get(DOCTAG_URL, "#"),
          'summaryText': doc.tags.get(LINKTAG_SUMMARYTEXT, ""),
          'images': doc.tags.get(TAG_IMAGES, "[]"),
          'lang': doc.tags.get(FEEDTAG_LANG, "")})
        docsAdded.append(docKey)
      else:
        self.duplicates.append(docKey)

      if doc.tags.get(FEEDTAG_CATEGORY):
         self.categories.append(doc.tags[FEEDTAG_CATEGORY])
      if doc.tags.get(FEEDTAG_COUNTRY):
         self.countries.append(doc.tags[FEEDTAG_COUNTRY])
      if doc.tags.get(FEEDTAG_LOCALE):
         self.locales.append(doc.tags[FEEDTAG_LOCALE])
      if doc.tags.get(PUBLISHERTAG_FRIENDLYID):
         self.publishers.append(doc.tags[PUBLISHERTAG_FRIENDLYID])
      if doc.tags.get(FEEDTAG_LANG):
         self.languages.append(doc.tags[FEEDTAG_LANG])

    nArticles = len(self.articles)
    #remove duplicates
    self.categories = _removeDuplicatesAndOutliers(self.categories, nArticles)
    self.countries = _removeDuplicatesAndOutliers(self.countries, nArticles)
    self.locales = _removeDuplicatesAndOutliers(self.locales, nArticles)
    self.publishers = list(set(self.publishers))
    self.languages = list(set(self.languages))
