class Cluster(set):
    """
    Represents a cluster(set of simmilar documents)
    """

    def __init__(self, docList):
        """
        Instantiates a new cluster object.
        Requires 'docList': a list of docIds comprising the cluster
        """

        set.__init__(self, docList)
