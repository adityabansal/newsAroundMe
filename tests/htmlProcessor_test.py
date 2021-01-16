import unittest
import newsApp.htmlProcessor as hp
from newsApp.link import Link

class HtmlProcessorTests(unittest.TestCase):

    def testHtmlProcessor(self):
        # get html
        link = Link(
            "https://www.deccanherald.com/city/bengaluru-crime/bitcoins-worth-rs-9-cr-seized-from-hacker-arrested-in-drugs-case-939549.html")
        html = link.getHtmlStatic()

        # process it
        result = hp.processHtml(
            "testJobId",
            html,
            ".content .field-name-body",
            [".content img"])

        # validate result
        self.assertEqual(len(result), 2)
        #expect processor to have extracted some text
        self.assertTrue(isinstance(result[0], str))
        self.assertGreater(len(result[0]), 100)
        #expect processor to find 1 image
        self.assertEqual(len(result[1]), 1)
