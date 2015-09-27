# helper db functions
import boto.dynamodb2
from boto.dynamodb2.table import Table

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

def getDbTable(connectionString):
    """
    Get a dynamo db table object using connection string
    """

    connectionParams = parseConnectionString(connectionString);

    return Table(
            connectionParams['name'],
            connection = getDbConnection(connectionParams));
