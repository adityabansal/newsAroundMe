import newsApp.constants as ct
from newsApp.feed import Feed
from newsApp.feedProcessor import getLinksFromWebFeed
import unittest

class FeedProcessorTests(unittest.TestCase):

  def testProcessWebFeed(self):
    dcHydFeedTags = {}
    dcHydFeedTags[ct.FEEDTAG_URL] = 'http://www.deccanchronicle.com/location/india/telangana/hyderabad'
    dcHydFeedTags[ct.FEEDTAG_LOCALE] = 'hyd'
    dcHydFeedTags[ct.FEEDTAG_IS_FEEDPAGE_STATIC] = 'true'
    dcHydFeedTags[ct.FEEDTAG_ENTRY_SELECTORS] = '[{"overall": ".storyList .SunChNewListing", ' + \
      '"title": ".col-sm-8 a", "titleText": ".col-sm-8 a h3"}]'
    dcHydFeed = Feed('dcHyd', dcHydFeedTags)

    links = getLinksFromWebFeed('testJob', dcHydFeed)
    self.assertEqual(len(links), 20, 'Expected 20 links found in web feed')
    for link in links:
        self.assertEqual(link.tags[ct.LINKTAG_ISPROCESSED], 'false')
        self.assertEqual(link.tags[ct.FEEDTAG_LOCALE], 'hyd')
        self.assertNotIn(ct.FEEDTAG_URL, link.tags)