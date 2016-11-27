import os
import logging
import json
import requests
import urllib

from googleapiclient.discovery import build

logger = logging.getLogger('translation')

MSTRANSLATE_LANGS = ['hi']
GOOGLE_LANGS = ['hi', 'mr']

def translateGoogle(jobInfo, text, fromLang, toLang = 'en'):
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

def _getMicrosoftAccessToken(jobInfo):
  try:
    auth_headers = {
      'Ocp-Apim-Subscription-Key': os.environ['MSTRANSLATE_AZUREKEY']
    }

    auth_url = 'https://api.cognitive.microsoft.com/sts/v1.0/issueToken'
    auth_token = requests.post(auth_url, headers = auth_headers).content

    logger.info(
      "Obtained MStranslate auth token through azure key. %s",
      jobInfo)
    return auth_token;
  except:
    args = {
      'client_id': os.environ['MSTRANSLATE_CLIENT_ID'],
      'client_secret': os.environ['MSTRANSLATE_CLIENT_SECRET'],
      'scope': 'http://api.microsofttranslator.com',
      'grant_type': 'client_credentials'
    }
    oauth_url = 'https://datamarket.accesscontrol.windows.net/v2/OAuth2-13'
    oauth_response = json.loads(
      requests.post(oauth_url, data=urllib.urlencode(args)).content)

    logger.info(
      "Obtained MStranslate auth token through old method. %s",
      jobInfo)
    return oauth_response['access_token'];

def translateMicrosoft(jobInfo, text, fromLang, toLang = 'en'):
  try:
    logger.info("Started microsoft translation. %s", jobInfo);

    # get the access token
    auth_token = _getMicrosoftAccessToken(jobInfo);

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
      'TranslateApiException' not in translation_result.content:
      logger.info("Completed microsoft translation. %s", jobInfo)
      return translation_result.content
    else:
      logger.info(
        "Microsoft translation call failed. Status code %i. Response: %s",
        translation_result.status_code,
        translation_result.content)
      return ""
  except Exception as e:
    logging.exception("Microsoft translation failed. %s", jobInfo)
    return ""

def translate(jobId, text, fromLang, toLang = 'en'):
  jobInfo = "fromLang: " + fromLang + " toLang: " + toLang \
     + " Job id: " + jobId

  # clip text if too long to save costs
  if len(text) > 600:
    text = text[:600]

  if fromLang in MSTRANSLATE_LANGS and fromLang in GOOGLE_LANGS:
    msResult = translateMicrosoft(jobInfo, text, fromLang, toLang)
    if len(msResult) > 0:
      return msResult
    else:
      return translateGoogle(jobInfo, text, fromLang, toLang)
  elif fromLang in GOOGLE_LANGS:
    return translateGoogle(jobInfo, text, fromLang, toLang)
