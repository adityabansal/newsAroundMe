from newsApp.link import Link
import unittest
import os

def getLocalFileUrl(relativePath):
    """
    Get the url of local file given it's relative path.
    """

    return "file://" + os.path.abspath(relativePath)

class LinkTests(unittest.TestCase):

    def testCreateLink(self):
        testLink = Link(getLocalFileUrl("tests/testData/testHtml.html"))
        html = testLink.getHtml();

    def testCreateDupLinks(self):
        link1 = Link("http://www.independent.co.uk/life-style/gadgets-and-tech/news/chinese-blamed-for-gmail-hacking-2292113.html")
        link2 = Link("http://www.independent.co.uk/life-style/gadgets-and-tech/news/2292113.html")
        self.failUnless(link1.id == link2.id);

if __name__ == '__main__':
    unittest.main()
