import unittest
import newsApp.htmlProcessor as hp
from newsApp.link import Link

class HtmlProcessorTests(unittest.TestCase):

    def testHtmlProcessor(self):
        # get html
        link = Link(
            "https://timesofindia.indiatimes.com/city/ahmedabad/iim-a-withdraws-eoi-to-raze-down-kahns-iconic-dorms/articleshow/80064663.cms")
        html = link.getHtmlStatic()

        # process it
        result = hp.processHtml(
            "testJobId",
            html,
            ".contentwrapper .ga-headlines",
            [".contentwrapper ._2suu5 img"])

        # validate result
        self.assertEqual(len(result), 2)
        #expect processor to have extracted some text
        self.assertTrue(isinstance(result[0], str))
        self.assertGreater(len(result[0]), 100)
        #expect processor to find 1 image
        self.assertEqual(len(result[1]), 1)
