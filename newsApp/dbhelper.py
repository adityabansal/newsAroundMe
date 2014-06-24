# helper db functions
import boto.dynamodb2

def parseConnectionString(connectionString):
    """
    Parses the table connection string. Returns a dict of parameters found.
    """

    return dict(item.split('=') for item in connectionString.split(';'))

def getDbConnection(connectionParams):
    """
    Get a dynamo db connection object using connection string params
    """

    connection = boto.dynamodb2.connect_to_region(
        connectionParams['region'],
        aws_access_key_id = connectionParams['accessKeyId'],
        aws_secret_access_key = connectionParams['secretAccessKey'])
    
    return connection
