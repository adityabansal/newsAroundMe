import logging
import requests
import urlparse

from lxml.etree import XMLSyntaxError
import lxml.html as lh
from lxml.html.clean import Cleaner

from imageProcessor import ImageProcessor

logger = logging.getLogger('htmlProcessor')
__imageProcessor = ImageProcessor()

def _isAbsolute(url):
    return bool(urlparse.urlparse(url).netloc)

def _addHttpToUrlIfNeeded(url):
    if not bool(urlparse.urlparse(url).scheme):
        return "http:" + url
    else:
        return url

def _getCompleteUrl(url, baseUrl):
    if not baseUrl:
        return url

    if _isAbsolute(url):
        return _addHttpToUrlIfNeeded(url)
    else:
        return urlparse.urljoin(baseUrl, url)

def _parseAndCleanHtml(rawHtml):
    # Parse html with lxml library
    parsedHtml = lh.fromstring(rawHtml);

    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True

    return cleaner.clean_html(parsedHtml)

def _extractText(html, textSelector):
    text = "";
    textDivs = html.cssselect(textSelector);
    for textDiv in textDivs:
        text += textDiv.text_content();
    return text;

def _extractImages(html, imageSelector, baseUrl):
    images = html.cssselect(imageSelector);
    return [_getCompleteUrl(img.attrib['src'], baseUrl) \
        for img in images if 'src' in img.attrib];

def _processImage(jobId, url):
    imageKey = __imageProcessor.processImage(jobId, url)

    if not imageKey:
      return "";
    else:
      return "images/" + imageKey;

def processHtml(jobId, rawHtml, textSelector, imageSelectors, baseUrl = None):
    """
    Process given html to extract out some text and images from it.

    Parameters:
        jobId: a job id to be used for tracing.
        rawHtml: the raw html to be processed.
        textSelector: css selector of element containing text to extract.
        imageSelectors: list of css selectors of elements containing images.
        baseUrl: the base url of page being parsed
    """

    # Parse html with lxml library
    try:
        parsedHtml = _parseAndCleanHtml(rawHtml);
    except XMLSyntaxError:
        logger.warning(
            "Could not parse page html. JobId: %s", jobId)
        return ("", [])

    # Extract out text
    text = _extractText(parsedHtml, textSelector)
    if (text == ""):
        logger.info(
            "Did not find any text for selector: %s. JobId: %s",
            textSelector,
            jobId);
    else:
        logger.info(
            "Sucessfully extracted out text from html. JobId: %s",
            jobId);

    # Extract out images
    images = [];
    for imageSelector in imageSelectors:
        images += _extractImages(parsedHtml, imageSelector, baseUrl);
    images = list(set(images)) # remove duplicates
    images = [_processImage(jobId, img) for img in images] #process images
    images = filter(None, images) #filter out unprocessed images
    logger.info("Extracted out %i images. JobId: %s", len(images), jobId);

    return (text, images);

def getSubHtmlEntries(jobId, fullHtml, selector):
    try:
        parsedHtml = _parseAndCleanHtml(fullHtml)
    except XMLSyntaxError:
        logger.warning(
            "Could not parse page html. JobId: %s", jobId)
        return []

    subHtmlEntries = parsedHtml.cssselect(selector)
    return [lh.tostring(entry) for entry in subHtmlEntries]

def extractLink(jobId, html, selector, baseUrl):
    try:
        parsedHtml = _parseAndCleanHtml(html)
    except XMLSyntaxError:
        logger.warning("Could not parse html. JobId: %s", jobId)
        return None

    titleElements = parsedHtml.cssselect(selector)
    if len(titleElements) > 0:
        titleElement = titleElements[0]

        titleText = titleElement.text_content()
        if not titleText:
            return None

        if 'href' in titleElement.attrib:
            return (
                _getCompleteUrl(titleElement.attrib['href'], baseUrl),
                titleText)

    logger.info("Could not extract link. JobId: %s", jobId)
    return None

def extractText(jobId, html, textSelector):
    try:
        parsedHtml = _parseAndCleanHtml(html)
    except XMLSyntaxError:
        logger.warning("Could not parse html. JobId: %s", jobId)
        return None

    # Extract out text
    return _extractText(parsedHtml, textSelector)