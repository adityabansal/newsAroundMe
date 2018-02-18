class Doc:
  """
  Represents a document.

  Each doc consists of an auto-generated id, original, processed content
  and some tags(which are key-value pairs to help in clustering).
  """

  def __init__(self, key, content, tags):
    """
    Instantiates a new document object.

    """

    self.key = key.upper();
    self.content = content;
    self.tags = tags;
