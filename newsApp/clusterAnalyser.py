from cluster import Cluster
import docHelper

class ClusterAnalyser:
  @staticmethod
  def isPolitical(clusterDocs):
    politicalTerms = [
      "Modi",
      "Rahul Gandhi",
      "BJP",
      "Mamta Bannerjee",
      "Kejriwal"]

    numOfPoliticalArticles = 0

    for doc in clusterDocs:
      numOfPoliticalTerms = 0
      docTitle = docHelper.getDocEnglishTitle(doc)

      for term in politicalTerms:
        if docTitle.lower().find(term.lower()) != -1:
          numOfPoliticalTerms = numOfPoliticalTerms + 1
    
      if numOfPoliticalTerms >= 2:
        numOfPoliticalArticles = numOfPoliticalArticles + 1

    if numOfPoliticalArticles >= 2:
      return True
    else:
      return False