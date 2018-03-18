# helper db functions
import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.s3.connection import S3Connection

from Crypto.Cipher import AES
import base64

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

def getS3Connection(connectionString):
    connectionParams = parseConnectionString(connectionString);

    connection = S3Connection(
        connectionParams['accessKeyId'],
        connectionParams['secretAccessKey']);

    return connection

def encryptSecret(value, encryptionKey):
    """
    Helper function to encrypt a secret before storing in database.
    """

    value = value + (16 - len(value) % 16) * '{'
    aesCipher = AES.new(encryptionKey, AES.MODE_ECB)
    cipherText = aesCipher.encrypt(value)
    return base64.b64encode(cipherText)

def decryptSecret(value, encryptionKey):
    """
    Helper function to decrypt a secret stored in database.
    """

    cipherText = base64.b64decode(value);
    aesCipher = AES.new(encryptionKey, AES.MODE_ECB)
    return aesCipher.decrypt(cipherText).rstrip('{');