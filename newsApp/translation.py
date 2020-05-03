import os
import logging
import json
import requests
import urllib.request, urllib.parse, urllib.error

from googleapiclient.discovery import build

logger = logging.getLogger('translation')

MSTRANSLATE_LANGS = ['hi', 'bn', 'ta', 'mr', 'gu', 'te', 'kn', 'ml']
GOOGLE_LANGS = ['hi', 'bn', 'ta', 'mr', 'gu', 'te', 'kn', 'ml']

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

    translation_args = {
      'text': text.encode(),
      'to': toLang,
      'from': fromLang
    }

    headers={'Authorization': 'Bearer ' + auth_token.decode()}
    translate_url = 'https://api.microsofttranslator.com/V2/Ajax.svc/Translate?'
    translation_result = requests.get(
      translate_url + urllib.parse.urlencode(translation_args),
      headers=headers)
    response = translation_result.content.decode()

    if translation_result.status_code == 200 and \
      'Exception:' not in response:
      logger.info("Completed microsoft translation. %s", jobInfo)
      return response
    else:
      logger.info(
        "Microsoft translation call failed. Status code %i. Response: %s",
        translation_result.status_code,
        response)
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
