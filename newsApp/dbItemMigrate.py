import os
import sys
from dbItemManager import DbItemManager
from dbItemManagerV2 import DbItemManagerV2
from loggingHelper import *

InitLogging()

def migrateData(fromEnvVariable, toEnvValue, fromTag):
	fromTableManager = DbItemManager(os.environ[fromEnvVariable])
	toTableManager = DbItemManagerV2(os.environ[toEnvValue])

	scanResults = fromTableManager.getEntriesWithTag(fromTag);
	for result in scanResults:
		logging.info("Getting item with id: %s from source table", result['itemId'])
		item = fromTableManager.get(result['itemId']);
		logging.info("Putting the item in destination table")
		toTableManager.put(item);
		logging.info("Successfully put the item in destination table");

if __name__ == '__main__':
	migrateData(sys.argv[1], sys.argv[2], sys.argv[3]);