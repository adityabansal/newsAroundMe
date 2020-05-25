import unittest
import newsApp.htmlProcessor as hp
from newsApp.link import Link

class HtmlProcessorTests(unittest.TestCase):

    def testHtmlProcessor(self):
        # get html
        link = Link(
            "https://timesofindia.indiatimes.com/city/vijayawada/rs-5000-cash-relief-for-priests/articleshow/75073058.cms")
        html = link.getHtmlStatic()

        # process it
        result = hp.processHtml(
            "testJobId",
            html,
            ".contentwrapper ._1IaAp ._3WlLe",
            [".contentwrapper ._2suu5 img"])

        # validate result
        self.assertEqual(len(result), 2)
        #expect processor to have extracted some text
        self.assertTrue(isinstance(result[0], str))
        self.assertGreater(len(result[0]), 100)
        #expect processor to find 1 image
        self.assertEqual(len(result[1]), 1)
