# helper db functions

def parseConnectionString(connectionString)
    """
    Parses the table connection string. Returns a dict of parameters found.
    """

    return dict(item.split('=') for item in connS.split(';'))

def getDbConnection(connectionParams):
    """
    Get a dynamo db connection object using connection string params
    """

    connection = boto.dynamodb2.connect_to_region(
        connectionParams['region'],
        connectionParams['accessKeyId'],
        connectionParams['secretAccessKey'])
    
    return connection
