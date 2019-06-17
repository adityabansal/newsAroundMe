import os
import logging
import json
import requests
import urllib

from googleapiclient.discovery import build

logger = logging.getLogger('translation')

MSTRANSLATE_LANGS = ['hi', 'bn', 'ta']
GOOGLE_LANGS = ['hi', 'bn', 'ta', 'mr', 'gu']

def translateGoogle(jobInfo, text, fromLang, toLang = 'en'):
  try:
    logger.info("Started google translation. %s", jobInfo)

    service = build('translate', 'v2',
            developerKey = os.environ['GOOGLE_DEV_KEY'])
    result = service.translations().list(
      source=fromLang,
      target=toLang,
      q=text).execute()['translations'][0]['translatedText']

    logger.info("Completed google translation. %s", jobInfo)
    return result
  except:
    logger.exception("Google translation failed. %s", jobInfo)
    return ""

def _getMicrosoftAccessToken(jobInfo):
  auth_headers = {
    'Ocp-Apim-Subscription-Key': os.environ['MSTRANSLATE_AZUREKEY']
  }

  auth_url = 'https://api.cognitive.microsoft.com/sts/v1.0/issueToken'
  get_auth_token_result = requests.post(auth_url, headers = auth_headers)
  if get_auth_token_result.status_code == 200:
    auth_token = get_auth_token_result.content

    logger.info(
      "Obtained MStranslate auth token through azure key. %s",
      jobInfo)
    return auth_token
  else:
    raise Exception(
      "Could not obtain microsoft access token. Response: " + \
      get_auth_token_result.content)

def translateMicrosoft(jobInfo, text, fromLang, toLang = 'en'):
  try:
    logger.info("Started microsoft translation. %s", jobInfo)

    # get the access token
    auth_token = _getMicrosoftAccessToken(jobInfo)

    # make the translate api call
    strText = text
    if isinstance(text, unicode):
      strText = text.encode('utf-8')

    translation_args = {
      'text': strText,
      'to': toLang,
      'from': fromLang
    }

    headers={'Authorization': 'Bearer '+ auth_token}
    translate_url = 'https://api.microsofttranslator.com/V2/Ajax.svc/Translate?'
    translation_result = requests.get(
      translate_url + urllib.urlencode(translation_args),
      headers=headers)

    if translation_result.status_code == 200 and \
      'Exception:' not in translation_result.content:
      logger.info("Completed microsoft translation. %s", jobInfo)
      return translation_result.content
    else:
      logger.info(
        "Microsoft translation call failed. Status code %i. Response: %s",
        translation_result.status_code,
        translation_result.content)
      return ""
  except Exception:
    logging.exception("Microsoft translation failed. %s", jobInfo)
    return ""

def translate(jobId, text, fromLang, toLang = 'en'):
  jobInfo = "fromLang: " + fromLang + " toLang: " + toLang \
     + " Job id: " + jobId

  # clip text if too long to save costs
  if len(text) > 800:
    text = text[:800]

  if fromLang in MSTRANSLATE_LANGS and fromLang in GOOGLE_LANGS:
    msResult = translateMicrosoft(jobInfo, text, fromLang, toLang)
    if len(msResult) > 0:
      return msResult
    else:
      return translateGoogle(jobInfo, text, fromLang, toLang)
  elif fromLang in GOOGLE_LANGS:
    return translateGoogle(jobInfo, text, fromLang, toLang)
