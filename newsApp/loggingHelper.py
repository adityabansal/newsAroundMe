#Initialize the logging.
# InitLogging() should be called at the startup of each process in Procfile

import logging

def InitLogging():
    """
    Initizalize the logging.
    """

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
