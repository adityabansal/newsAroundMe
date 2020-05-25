from newsApp.constants import *
from newsApp.feedManager import FeedManager
from newsApp.feed import Feed

import time
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
        self.assertDictEqual(retrievedFeed.tags, testFeed.tags)
        self.assertEqual(retrievedFeed.id, testFeed.id)

        # delete feed. trying to get feed should raise an exception
        testFeedManager.delete(testFeed.id)
        self.assertRaises(Exception, testFeedManager.get, testFeed.id)

    def testPutFeedAndUpdate(self):
        """
        Put a feed and then call updateFeedOnSuccessfullPoll.
        """

        testFeedManager = FeedManager()
        testFeed = Feed(
            'testFeedName',
            {
                FEEDTAG_TYPE : FEEDTYPE_WEBPAGE,
                FEEDTAG_URL : "https://newsStite.com",
                FEEDTAG_LASTPOLLTIME: 100,
                FEEDTAG_POLLFREQUENCY: 12,
                "extraTag" : "true"
            })

        # put the feed
        testFeedManager.put(testFeed)

        # add a tag and put the feed again
        testFeed.tags[FEEDTAG_LASTPUBDATE] = 500
        testFeedManager.updateFeedOnSuccessfullPoll(testFeed)

        # updateFeedOnSuccessfullPoll should have updated feed pollTime tags
        self.assertAlmostEqual(testFeed.tags[FEEDTAG_LASTPOLLTIME], int(time.time()), delta=5)
        self.assertGreaterEqual(testFeed.tags[FEEDTAG_NEXTPOLLTIME], testFeed.tags[FEEDTAG_LASTPOLLTIME])

        # get the feed and validate it's same as one you put the 2nd time
        retrievedFeed = testFeedManager.get(testFeed.id)
        self.assertTrue(retrievedFeed.tags == testFeed.tags)
        self.assertTrue(retrievedFeed.id == testFeed.id)

        # Cleanup - delete feed.
        testFeedManager.delete(testFeed.id)
