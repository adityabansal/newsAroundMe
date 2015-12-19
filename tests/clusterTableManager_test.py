import time
import unittest

from newsApp.clusterTableManager import ClusterTableManager
from newsApp.cluster import Cluster

class ClusterTableManagerTests(unittest.TestCase):

  def setUp(self):
    # construct the mock clusters to put
    clusters = [
      Cluster(['d1', 'd2', 'd3', 'd4', 'd5']),
      Cluster(['e1'])
    ]

    clusters[0].categories = str(['sports', 'national'])
    clusters[1].categories = str(['business'])

    clusters[0].countries = str(['india', 'nepal'])
    clusters[1].countries = str(['india'])

    clusters[0].locales = str(['bangalore', 'chennai'])
    clusters[1].locales = str(['hyderabad'])

    clusters[0].publishers = str(['TOI', 'Hindu', 'Deccan Herald'])
    clusters[1].publishers = str(['Firstpost'])

    clusters[0].languages = str(['en'])
    clusters[1].languages = str(['es'])

    self.clusters = clusters

    self.clusterTableManager = ClusterTableManager()
    self.clusterTableManager.createFreshTable()

    # wait for table to get created and add entries
    time.sleep(10);
    self.clusterTableManager.addClusters(self.clusters)

  def tearDown(self):
    self.clusterTableManager.deleteTable()

  def testQueries(self):
    #simple get
    result = self.clusterTableManager.getCluster(self.clusters[0].id)
    self.assertEqual(result.id, self.clusters[0].id)
    result = self.clusterTableManager.getCluster(self.clusters[1].id)
    self.assertEqual(result.id, self.clusters[1].id)

    #query by category & country
    result = self.clusterTableManager.queryByCategoryAndCountry('sports', 'nepal')
    self.assertEqual(list(result)[0].id, self.clusters[0].id)
    result = self.clusterTableManager.queryByCategoryAndCountry('business', 'india')
    self.assertEqual(list(result)[0].id, self.clusters[1].id)

    #query by locale
    result = self.clusterTableManager.queryByLocale('bangalore')
    self.assertEqual(list(result)[0].id, self.clusters[0].id)
    result = self.clusterTableManager.queryByLocale('chennai')
    self.assertEqual(list(result)[0].id, self.clusters[0].id)
    result = self.clusterTableManager.queryByLocale('hyderabad')
    self.assertEqual(list(result)[0].id, self.clusters[1].id)
