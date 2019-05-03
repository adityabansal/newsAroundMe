import hashlib
import json
from operator import itemgetter
import Queue
from threading import Thread

from constants import *
from docHelper import getDocComparisionScore
from docManager import DocManager
import textHelper as th

DOC_DUPLICATION_THRESHOLD = 0.80
def _removeDuplicatesAndOutliers(items, articleCount):
  d = {}
  for item in items:
    if item in d:
      d[item] = d[item] + 1
    else:
      d[item] = 1

  return [item for item in d.keys() if d[item] > 0.4 * articleCount]

def _isDuplicateArticle(doc, docsAdded):
  for addedDoc in docsAdded:
    distance = getDocComparisionScore("processCluster", doc, addedDoc)
    if distance > DOC_DUPLICATION_THRESHOLD:
      return True

  return False

def _getDocsInParallel(docKeys):
  que = Queue.Queue()
  threads_list = list()
  docManager = DocManager()
  for docKey in docKeys:
    t = Thread(
      target=lambda q, arg1: q.put(docManager.get(arg1)),
      args=(que, docKey))
    t.start()
    threads_list.append(t)

  for t in threads_list:
    t.join()

  docs = list()
  while not que.empty():
    docs.append(que.get())

  return docs

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

    docList.sort()
    self.id = hashlib.md5("-".join(docList)).hexdigest().upper()
    self.isCurrent = 'unknown'
    self.isProcessed = False

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

    docKeys = list(super(Cluster, self).__iter__())
    docs = _getDocsInParallel(docKeys)
    docsAdded = []
    for doc in docs:
      docKey = doc.key
      if not _isDuplicateArticle(doc, docsAdded):
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
        docsAdded.append(doc)
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
    self.isProcessed = True

  def deserializeFromString(self, clusterString):
    clusterDict = json.loads(clusterString)
    self.__init__(clusterDict['docKeys'])
    self.articles = clusterDict['articles']
    self.categories = clusterDict['categories']
    self.countries = clusterDict['countries']
    self.locales = clusterDict['locales']
    self.publishers = clusterDict['publishers']
    self.languages = clusterDict['languages']
    self.duplicates = clusterDict['duplicates']
    self.isCurrent = clusterDict['isCurrent']
    self.lastPubTime = float(clusterDict['lastPubTime'])
    self.isProcessed = True

  def serializeToString(self):
    """
    Serialize cluster to string. Only works for processed clusters
    """

    if not self.isProcessed:
      raise ValueError('Only processed clusters can be serialized')

    return json.dumps({
      'clusterId': self.id,
      'docKeys': list(super(Cluster, self).__iter__()),
      'articles': self.articles,
      'categories': self.categories,
      'countries': self.countries,
      'locales': self.locales,
      'publishers': self.publishers,
      'languages': self.languages,
      'duplicates': self.duplicates,
      'isCurrent': self.isCurrent,
      'lastPubTime': self.lastPubTime
    })