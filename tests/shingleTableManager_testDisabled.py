import time
import unittest

from newsApp.shingleTableManager import ShingleTableManager

class ShingleTableManagerTests(unittest.TestCase):

    def testCreateTableAndUse(self):
        testShingleTableManager = ShingleTableManager()

        # create the table
        testShingleTableManager.createFreshTable();

        # wait for table to get created and add entries
        time.sleep(10);
        testShingleTableManager.addEntries('testDoc1', ['a', 'b', 'c']);
        testShingleTableManager.addEntries('testDoc2', ['a', 'b', 'd']);

        #query by shingle
        result = testShingleTableManager.queryByShingle('a')
        self.assertTrue(result, ['testDoc1', 'testDoc2'])
        result = testShingleTableManager.queryByShingle('d')
        self.assertTrue(result, ['testDoc2'])

        #query by docId
        result = testShingleTableManager.queryByDocId('testDoc1')
        self.assertTrue(result, ['a', 'b', 'c'])

        #delete by docId
        testShingleTableManager.cleanUpDocShingles('testDoc1')
        self.assertTrue(result, [])
