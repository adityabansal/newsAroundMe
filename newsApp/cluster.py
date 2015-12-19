import hashlib

from constants import *
from docManager import DocManager

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

  def expand(self):
    """
    Expands the cluster to include metadata of consisting documents,
    and overall metadata of cluster like category, location, feeds, etc.
    """

    self.categories = []
    self.countries = []
    self.locales = []
    self.publishers = []
    self.languages = []
    self.articles = []

    docManager = DocManager()
    for docKey in super(Cluster, self).__iter__():
      doc = docManager.get(docKey)

      self.articles.append({
          'title': doc.tags.get(LINKTAG_TITLE, ""),
          'publisher': doc.tags.get(TAG_PUBLISHER_DETAILS, ""),
          'link': doc.tags.get(DOCTAG_URL, "#"),
          'summaryText': doc.tags.get(LINKTAG_SUMMARYTEXT, ""),
          'images': doc.tags.get(TAG_IMAGES, "[]"),
          'lang': doc.tags.get(FEEDTAG_LANG, "")})

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

    #remove duplicates
    self.categories = list(set(self.categories))
    self.countries = list(set(self.countries))
    self.locales = list(set(self.locales))
    self.publishers = list(set(self.publishers))
    self.languages = list(set(self.languages))
