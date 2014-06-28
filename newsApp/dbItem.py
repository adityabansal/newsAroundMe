class DbItem:
  """
  Represents a generic item which can be added/updated/deleted/retrieved to a
  database.

  Each dbItem consists of a unique identifier and a set of tags(which are simple key-value pairs).
  """

  def __init__(self, id, tags=None):
    """
    Instantiates a new dbItem object.

    Requires a 'id' parameter which should be a simple representing a unique
    identifier for the dbItem.

    Optionally accepts a 'tags' parameter which should be a dictionary of
    key-value pairs. e.g. you can have a tag for the language of dbItem.
    """

    self.id = id
    self.tags = tags
