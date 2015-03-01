from newsApp.feedManager import FeedManager
from newsApp.feed import Feed
import unittest

class FeedManagerTests(unittest.TestCase):

    def testPutGetDelete(self):
        testFeedManager = FeedManager()
        testFeed = Feed(
            'testFeedName',
            { 'tag1' : 'value1', 'tag2' : 'value2' })

        # put the feed
        testFeedManager.put(testFeed)

        # get the feed and validate it's same as one you put
        retrievedFeed = testFeedManager.get(testFeed.id)
        self.failUnless(retrievedFeed.tags == testFeed.tags)
        self.failUnless(retrievedFeed.id == testFeed.id)

        # delete feed. trying to get feed should raise an exception
        testFeedManager.delete(testFeed.id)
        self.assertRaises(Exception, testFeedManager.get, testFeed.id)

    def testPutFeedTwice(self):
        """
        Puttng a feed twice possibly with different tags should work.
        """

        testFeedManager = FeedManager()
        testFeed = Feed(
            'testFeedName',
            { 'tag1' : 'value1', 'tag2' : 'value2' })

        # put the feed
        testFeedManager.put(testFeed)

        # add a tag and put the feed again
        testFeed.tags['tag3'] = 'value3'
        testFeedManager.put(testFeed)

        # get the feed and validate it's same as one you put the 2nd time
        retrievedFeed = testFeedManager.get(testFeed.id)
        self.failUnless(retrievedFeed.tags == testFeed.tags)
        self.failUnless(retrievedFeed.id == testFeed.id)

        # Cleanup - delete feed.
        testFeedManager.delete(testFeed.id)
