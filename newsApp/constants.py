# define some constants used across modules.

LANG_ENGLISH = 'en'
LANG_ENGLISH_METADATA = {'id': LANG_ENGLISH, 'displayName': 'English'}
LANG_HINDI = 'hi'
LANG_HINDI_METADATA = {'id': LANG_HINDI, 'displayName': 'Hindi'}
LANG_MARATHI = "mr"
LANG_MARATHI_METADATA = {'id': LANG_MARATHI, 'displayName': 'Marathi'}

# common tag names
TAG_PUBLISHER = 'publisher'
TAG_PUBLISHER_DETAILS = 'publisherDetails'
TAG_IMAGES = 'images'

#doc tags
DOCTAG_URL = 'url'
DOCTAG_TRANSLATED_TITLE = 'translatedTitle'
DOCTAG_TRANSLATED_SUMMARYTEXT = 'translatedSummaryText'
DOCTAG_TRANSLATED_CONTENT = 'translatedContent'

FEEDTYPE_RSS = 'rss'
FEEDTYPE_WEBPAGE = 'webPage'

# feed tags
FEEDTAG_TYPE = 'type'
FEEDTAG_NEXTPOLLTIME = 'nextPollTime'
FEEDTAG_POLLFREQUENCY = 'pollFrequency'
FEEDTAG_LASTPOLLTIME = 'lastPollTime'
FEEDTAG_URL = 'feedUrl'
FEEDTAG_LASTPUBDATE = 'lastPubDate'
FEEDTAG_CATEGORY = 'category'
FEEDTAG_COUNTRY = 'country'
FEEDTAG_LANG = 'lang'
FEEDTAG_LOCALE = 'locale'
FEEDTAG_ENTRY_SELECTORS = 'entrySelectors'
FEEDTAG_DO_NOT_CLUSTER = 'doNotCluster'

# link tags
LINKTAG_ISPROCESSED = 'isProcessed'
LINKTAG_DOCKEY = 'docKey'
LINKTAG_SUMMARY = 'summary'
LINKTAG_SUMMARYTEXT = 'summaryText'
LINKTAG_SUMMARYIMAGES = 'summaryImages'
LINKTAG_PUBTIME = 'pubtime'
LINKTAG_TITLE = 'title'

# publisher tags
PUBLISHERTAG_TEXTSELECTOR = 'textSelector'
PUBLISHERTAG_IMAGESELECTORS = 'imageSelectors'
PUBLISHERTAG_HOMEPAGE = 'homepage'
PUBLISHERTAG_NAME = 'name'
PUBLISHERTAG_FRIENDLYID = 'friendlyId'

PUBLISHER_DETAILS_HOMEPAGE = 'homepage'
PUBLISHER_DETAILS_NAME = 'name'
PUBLISHER_DETAILS_FRIENDLYID = 'id'

#job names
JOB_PROCESSFEED = 'processFeed'
JOBARG_PROCESSFEED_FEEDID = 'feedId'
JOB_PROCESSLINK = 'processLink'
JOBARG_PROCESSLINK_LINKID = 'linkId'
JOB_PARSEDOC = 'parseDoc'
JOBARG_PARSEDOC_DOCID = 'docId'
JOB_GETCANDIDATEDOCS = 'getCandidateDocs'
JOBARG_GETCANDIDATEDOCS_DOCID = 'docId'
JOB_COMPAREDOCS = 'compareDocs'
JOBARG_COMPAREDOCS_DOC1ID = 'doc1'
JOBARG_COMPAREDOCS_DOC2ID ='doc2'
JOB_CLUSTERDOCS = 'clusterDocs'
JOB_UPDATEDBTHROUGHPUT = 'updateDbThroughput'
JOB_UPDATEDBTHROUGHPUT_CONNECTIONSTRING = 'connectionString'
JOB_UPDATEDBTHROUGHPUT_READTHOUGHPUT = 'readThroughput'
JOB_UPDATEDBTHROUGHPUT_WRITETHOUGHPUT = 'writeThroughput'
JOB_UPDATEDBTHROUGHPUT_INDEXNAME = 'indexName'
JOB_CLEANUPDOC = 'cleanupDoc'
JOBARG_CLEANUPDOC_DOCID = 'docId'
JOB_CLEANUPDOCSHINGLES = 'cleanupDocShingles'
JOBARG_CLEANUPDOCSHINGLES_DOCID = 'docId'
JOB_CLEANUPDOCENTITIES = 'cleanupDocEntities'
JOBARG_CLEANUPDOCENTITIES_DOCID = 'docId'
JOB_CLEANUPDOCDISTANCES = 'cleanupDocDistances'
JOBARG_CLEANUPDOCDISTANCES_DOCID = 'docId'
JOB_PROCESSNEWCLUSTER = 'processNewCluster'
JOBARG_PROCESSNEWCLUSTER_CLUSTER = 'cluster'

#clustering job states
CLUSTER_STATE_INITIALIZED = 'initialized'
CLUSTER_STATE_NEW = 'new'
CLUSTER_STATE_STARTED = 'started'
CLUSTER_STATE_COMPLETED = 'completed'

#cluster filters
CLUSTERS_FILTER_LANGUAGES = 'languages'

# global parameters
CLUSTERING_DOC_AGE_LIMIT = 1

#UX metadata
ALLOWED_CATEGORIES = ['sports', 'business', 'national', 'world']
ALLOWED_COUNTRIES = ['India']
AVAILABLE_LANGUAGES = [
	LANG_ENGLISH_METADATA,
	LANG_HINDI_METADATA,
	LANG_MARATHI_METADATA
]

LOCATION_METADATA = [
  {'displayName': 'Delhi', 'icon': '/static/delhi.svg', 'value': 'delhi', 'lat': 28.70, 'long': 77.10, 'languages': [LANG_ENGLISH_METADATA, LANG_HINDI_METADATA]},
  {'displayName': 'Mumbai', 'icon': '', 'value': 'mumbai', 'lat': 19.08, 'long': 72.88, 'languages': [LANG_ENGLISH_METADATA]},
  {'displayName': 'Bangalore', 'icon': '/static/city.svg', 'value': 'bangalore', 'lat': 12.97, 'long': 77.59, 'languages': [LANG_ENGLISH_METADATA]},
  {'displayName': 'Chennai', 'icon': '', 'value': 'chennai', 'lat': 13.08, 'long': 80.27, 'languages': [LANG_ENGLISH_METADATA]},
  {'displayName': 'Kolkata', 'icon': '', 'value': 'kolkata', 'lat': 22.57, 'long': 88.36, 'languages': [LANG_ENGLISH_METADATA]},
  {'displayName': 'Hyderabad', 'icon': '', 'value': 'hyderabad', 'lat': 17.39, 'long': 78.49, 'languages': [LANG_ENGLISH_METADATA]},
  {'displayName': 'Pune', 'icon': '', 'value': 'pune', 'lat': 18.52, 'long': 73.86, 'languages': [LANG_ENGLISH_METADATA, LANG_MARATHI_METADATA]}]
for location in LOCATION_METADATA:
	location['title'] = location['displayName'] + ' News - newsAroundMe';
	location['description'] = 'Latest local news from '+ location['displayName'] + '.';