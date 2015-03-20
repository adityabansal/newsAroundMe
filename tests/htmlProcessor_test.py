import unittest
import urllib
import newsApp.htmlProcessor as hp

class HtmlProcessorTests(unittest.TestCase):

    def testHtmlProcessor(self):
        # get html
        sock = urllib.urlopen("http://timesofindia.indiatimes.com/india/Tweets-on-beef-land-actor-Rishi-Kapoor-in-controversy/articleshow/46628197.cms");
        html = sock.read();
        sock.close();

        # process it
        result = hp.processHtml(
            html,
            "div#storydiv .Normal",
            ["div#storydiv .mainimg1 img"]);

        # validate result
        self.failUnless(len(result), 2);
        self.failUnless(len(result[1]), 1); #expect processor to find 1 image
