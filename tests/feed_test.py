from newsApp.feed import Feed
import unittest

class FeedTests(unittest.TestCase):

    def testCreateFeed(self):
        testFeed = Feed("feedId")
        self.assertTrue(testFeed.id == "feedId")

if __name__ == '__main__':
    unittest.main()
