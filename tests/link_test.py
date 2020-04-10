from newsApp.link import Link
import unittest
import os

class LinkTests(unittest.TestCase):

    def testCreateLink(self):
        testLink = Link(
            "http://timesofindia.indiatimes.com/city/hyderabad/deputation-rule-hyderabad-calling-for-andhra-pradesh-staff/articleshow/60979298.cms")
        self.assertTrue(testLink.checkExistence(), 'Link exists')

        testLink = Link("http://dfshkjbslabfhjgsdaltyuxz.com/sf")
        self.assertFalse(testLink.checkExistence(), 'Link doesnt exist')


if __name__ == '__main__':
    unittest.main()
