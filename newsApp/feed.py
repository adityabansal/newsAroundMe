class Feed:
  """
  Represents a web feed.

  Each feed consists of a unique identifier and a set of tags(key-value pairs).
  """

  def __init__(self, id, tags=None):
    """
    Instantiates a new feed object.

    Requires a 'identifier' parameter which should be a simple string of a
    unique identifier for the feed.

    Optionally accepts a 'tags' parameter which should be a dictionary of
    key-value pairs. e.g. you can have a tag for the language of feed.
    """

    self.id = id
    self.tags = tags
