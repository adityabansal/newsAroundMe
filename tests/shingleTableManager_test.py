import time
import unittest

from newsApp.shingleTableManager import ShingleTableManager

class ShingleTableManagerTests(unittest.TestCase):

    def testCreateTableAndUse(self):
        testShingleTableManager = ShingleTableManager()

        # wait for table to get created and add entries
        testShingleTableManager.addEntries('testDoc1', ['ab', 'bcd', 'c'])
        testShingleTableManager.addEntries('testDoc2', ['ab', 'bc', 'd'])

        #query by shingle
        result = testShingleTableManager.queryByShingle('ab')
        self.assertTrue(result, ['testDoc1', 'testDoc2'])
        result = testShingleTableManager.queryByShingle('d')
        self.assertTrue(result, ['testDoc2'])

        #query by docId
        result = testShingleTableManager.queryByDocId('testDoc1')
        self.assertTrue(result, ['ab', 'bcd', 'c'])

        #delete by docId
        testShingleTableManager.cleanUpDocShingles('testDoc1')
        result = testShingleTableManager.queryByDocId('testDoc1')
        self.assertTrue(result, [])
