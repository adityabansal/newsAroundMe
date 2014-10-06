import xml.etree.ElementTree as ET
import sys

from newsApp.feed import Feed
from newsApp.feedManager import FeedManager

def populateFeeds():
    tree = ET.parse(sys.argv[1])
    root = tree.getroot()

    for child in root:
        feedTags = child.attrib
        feedId = feedTags['id'];
        feedTags.pop('id', None);

        feed = Feed(feedId, feedTags);
        feedManager = FeedManager()
        feedManager.put(feed);

if __name__ == '__main__':
    populateFeeds()
