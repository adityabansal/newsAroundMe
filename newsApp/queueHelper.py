# helper queue functions
import boto.sqs
from boto.sqs.message import Message

def __parseConnectionString(connectionString):
    """
    Parses the queue connection string. Returns a dict of parameters found.
    """

    return dict(item.split('=') for item in connectionString.split(';'))

def getQueue(connectionString):
    """
    Get a queue object using a connection string.
    """

    connectionParams = __parseConnectionString(connectionString)

    connection = boto.sqs.connect_to_region(
        connectionParams['region'],
        aws_access_key_id = connectionParams['accessKeyId'],
        aws_secret_access_key = connectionParams['secretAccessKey'])
    
    return connection.get_queue(connectionParams['name'])

def enqueueMessage(queue, messageString):
    """
    Enqueue a message into the queue
    """

    m = Message()
    m.set_body(messageString)
    queue.write(m)

def dequeueMessage(queue):
    """
    Dequeue  a single  message from the queue.
    Returns None if no message found.
    """

    m = queue.read()
    if m is None:
        return None
    else:
        queue.delete_message(m)
        return m.get_body()

def dequeMessageWithFilter(queue, filterFunction):
    messages = queue.get_messages(num_messages=10, visibility_timeout=5)
    if not messages:
        return None

    for m in messages:
        mBody = m.get_body()
        isAllowedJob = filterFunction(mBody)
        if isAllowedJob:
            queue.delete_message(m)
            return mBody
