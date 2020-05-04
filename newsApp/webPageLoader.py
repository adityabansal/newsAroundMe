import requests
import logging
import signal
import time
from selenium import webdriver

logger = logging.getLogger('webPageLoader')

def getHtmlStatic(url, max_retries = 1):
    nRetries = 0
    headers = {'User-Agent': 'newsbot'}
    while (True):
        try:
            return requests.get(url, headers = headers, timeout = 20).text
        except Exception as e:
            if (nRetries >= max_retries):
                raise e
            else:
                time.sleep(5)
                nRetries = nRetries + 1

def loadPageAndGetHtml(url):
  pageHtml = ""

  try:
    with warnings.catch_warnings():
      #ignore PhantomJS deprecated warning.
      warnings.simplefilter("ignore")

      driver = webdriver.PhantomJS(executable_path='node_modules/phantomjs-prebuilt/bin/phantomjs')
      driver.implicitly_wait(30)
      driver.set_page_load_timeout(30)

    try:
      driver.get(url)
      pageHtml = driver.page_source
    finally:
      try:
        driver.service.process.send_signal(signal.SIGTERM)
        driver.quit()
      except:
        pass
  except:
    logger.warning("Could not load page with url %s through selenium", url)
    pass

  if not pageHtml:
    pageHtml = getHtmlStatic(url)

  return pageHtml
