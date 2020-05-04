import jellyfish

class EncodedEntity:
  """
  Represents a phonetically encoded entity
  """

  def __init__(self, plainEntity):
    """
    Instantiates a new encoded entity object.
    Requires 'plainEntity': plain text entity to encode
    """

    self.plain = plainEntity
    self.encoded = jellyfish.metaphone(plainEntity)