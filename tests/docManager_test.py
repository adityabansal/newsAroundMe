import unittest

from newsApp.doc import Doc
from newsApp.docManager import DocManager

class DocManagerTests(unittest.TestCase):

    def testPutGetDeleteDoc(self):
        testDocManager = DocManager();
        testDoc = Doc(
            'unittest',
            'docContent',
            { 'tag1' : 'value1', 'tag2' : 'value2' });

        # put the doc
        testDocManager.put(testDoc);

        # get the doc and validate it's same as one you put
        retrievedDoc = testDocManager.get(testDoc.key)
        self.failUnless(retrievedDoc.key == testDoc.key)
        self.failUnless(retrievedDoc.content == testDoc.content)
        self.failUnless(retrievedDoc.tags == testDoc.tags)

        # get new doc keys
        retrievedDocKeys = testDocManager.getNewDocKeys(1)
        self.failUnless(len(list(retrievedDocKeys)) == 1)

        # delete doc
        testDocManager.delete(testDoc.key)

