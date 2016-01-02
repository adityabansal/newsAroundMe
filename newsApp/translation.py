import os
import logging

from googleapiclient.discovery import build

logger = logging.getLogger('translation')

def translate(jobId, text, fromLang, toLang = 'en'):
  jobInfo = "fromLang: " + fromLang + " toLang: " + toLang \
     + " Job id: " + jobId

  try:
    logger.info("Started google translation. %s", jobInfo);

    service = build('translate', 'v2',
            developerKey = os.environ['GOOGLE_DEV_KEY'])
    result = service.translations().list(
      source=fromLang,
      target=toLang,
      q=text).execute()['translations'][0]['translatedText']

    logger.info("Completed google translation. %s", jobInfo)
    return result
  except:
    logger.info("Google translation failed. %s", jobInfo)
    return ""
