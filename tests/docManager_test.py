import unittest

from newsApp.doc import Doc
from newsApp.docManager import DocManager

class DocManagerTests(unittest.TestCase):

    def testPutGetDeleteDoc(self):
        testDocManager = DocManager()
        testDoc = Doc(
            'unittest',
            'docContent',
            { 'tag1' : 'value1', 'tag2' : 'value2' })

        # put the doc
        testDocManager.put(testDoc)

        # get the doc and validate it's same as one you put
        retrievedDoc = testDocManager.get(testDoc.key)
        self.assertTrue(retrievedDoc.key == testDoc.key)
        self.assertTrue(retrievedDoc.content == testDoc.content)
        self.assertTrue(retrievedDoc.tags == testDoc.tags)

        # delete doc
        testDocManager.delete(testDoc.key)

