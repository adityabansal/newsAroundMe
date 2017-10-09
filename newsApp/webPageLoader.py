import requests
import logging
import signal
from selenium import webdriver

logger = logging.getLogger('webPageLoader')

def getHtmlStatic(url):
	return requests.get(url).text;

def loadPageAndGetHtml(url):
  pageHtml = "";

  try:
    driver = webdriver.PhantomJS(executable_path='node_modules/phantomjs-prebuilt/bin/phantomjs')
    driver.implicitly_wait(30)
    driver.set_page_load_timeout(30)
    driver.get(url)
    pageHtml = driver.page_source;
    try:
      driver.service.process.send_signal(signal.SIGTERM)
      driver.quit()
    except:
      pass;
  except:
    logger.warning("Could not load page with url %s through selenium", url);
    pass;

  if not pageHtml:
    pageHtml = getHtmlStatic(url)

  return pageHtml;