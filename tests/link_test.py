from newsApp.link import Link
import unittest
import os

class LinkTests(unittest.TestCase):

    def testCreateLink(self):
        testLink = Link(
            "http://timesofindia.indiatimes.com/city/hyderabad/deputation-rule-hyderabad-calling-for-andhra-pradesh-staff/articleshow/60979298.cms")
        html = testLink.getHtml();

if __name__ == '__main__':
    unittest.main()
