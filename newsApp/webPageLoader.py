import requests
import logging
from selenium import webdriver

logger = logging.getLogger('webPageLoader')

def loadPageAndGetHtml(url):
  pageHtml = "";

  driver = webdriver.PhantomJS(executable_path='node_modules/phantomjs-prebuilt/bin/phantomjs')
  driver.implicitly_wait(30)
  driver.set_page_load_timeout(30)
  try:
    driver.get(url)
    pageHtml = driver.page_source;
  except:
    logger.warning("Could not load page with url %s through selenium", url);
    pageHtml = requests.get(url).text
  finally:
    driver.quit()

    return pageHtml;