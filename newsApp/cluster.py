import hashlib
from operator import itemgetter

from constants import *
from docManager import DocManager
from distanceTableManager import DistanceTableManager
import textHelper as th

DOC_DUPLICATION_THRESHOLD = 0.85
def _removeDuplicatesAndOutliers(items, articleCount):
  d = {}
  for item in items:
    if item in d:
      d[item] = d[item] + 1
    else:
      d[item] = 1

  return [item for item in d.keys() if d[item] > 0.4 * articleCount]

def _isDuplicateArticle(docKey, docsAdded):
  distanceTableManager = DistanceTableManager()

  for addedDoc in docsAdded:
    distance = distanceTableManager.getDistance(docKey, addedDoc)
    if distance > DOC_DUPLICATION_THRESHOLD:
      return True

  return False

def _getDocTitle(doc):
  title = doc.tags.get(LINKTAG_TITLE, "").strip()
  if doc.tags[FEEDTAG_LANG] == LANG_ENGLISH:
    return th.removeNonAsciiChars(title)
  else:
    return title

def _getDocSummary(doc):
  summary = doc.tags.get(LINKTAG_SUMMARYTEXT, "").strip()
  if doc.tags[FEEDTAG_LANG] == LANG_ENGLISH:
    return th.removeNonAsciiChars(summary)
  else:
    return summary

def _getImagesForDoc(doc):
  images = doc.tags.get(TAG_IMAGES, [])
  if not images:
    images = []

  summaryImages = doc.tags.get(LINKTAG_SUMMARYIMAGES, [])
  if not summaryImages:
    summaryImages = []

  return list(set(images + summaryImages))

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
    self.id = hashlib.md5("-".join(docList)).hexdigest().upper()
    self.isCurrent = 'unknown'

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
    self.lastPubTime = 0

    docManager = DocManager()
    docsAdded = []
    for docKey in super(Cluster, self).__iter__():
      doc = docManager.get(docKey)

      if not _isDuplicateArticle(docKey, docsAdded):
        # note that this gets passed all the way till browser
        # don't add any internal stuff here
        self.articles.append({
          'id': docKey,
          'title': _getDocTitle(doc),
          'publisher': doc.tags.get(TAG_PUBLISHER_DETAILS, ""),
          'link': doc.tags.get(DOCTAG_URL, "#"),
          'summaryText': _getDocSummary(doc),
          'highlights': doc.tags.get(LINKTAG_HIGHLIGHTS, []),
          'images': _getImagesForDoc(doc),
          'lang': doc.tags.get(FEEDTAG_LANG, ""),
          'publishedOn': doc.tags.get(LINKTAG_PUBTIME, 0)
        })
        docsAdded.append(docKey)
      else:
        self.duplicates.append(docKey)

      if doc.tags.get(FEEDTAG_CATEGORY):
        self.categories.append(doc.tags[FEEDTAG_CATEGORY])
      if doc.tags.get(FEEDTAG_COUNTRY):
        self.countries.append(doc.tags[FEEDTAG_COUNTRY])
      if doc.tags.get(FEEDTAG_LOCALE):
        self.locales.append(doc.tags[FEEDTAG_LOCALE])
      if doc.tags.get(TAG_PUBLISHER):
        self.publishers.append(doc.tags[TAG_PUBLISHER])
      if doc.tags.get(FEEDTAG_LANG):
        self.languages.append(doc.tags[FEEDTAG_LANG])
      if doc.tags.get(LINKTAG_PUBTIME, 0) > self.lastPubTime:
        self.lastPubTime =  doc.tags.get(LINKTAG_PUBTIME)

    nArticles = len(self.articles) + len(self.duplicates)
    self.articles.sort(key=itemgetter('publishedOn'), reverse=True)

    #remove duplicates
    self.categories = _removeDuplicatesAndOutliers(self.categories, nArticles)
    self.countries = _removeDuplicatesAndOutliers(self.countries, nArticles)
    self.locales = _removeDuplicatesAndOutliers(self.locales, nArticles)
    self.publishers = list(set(self.publishers))
    self.languages = list(set(self.languages))
