
import logging
import requests
import urlparse

from lxml.etree import XMLSyntaxError,ParserError
import lxml.html as lh
from lxml.html.clean import Cleaner

from imageProcessor import ImageProcessor

logger = logging.getLogger('htmlProcessor')
__imageProcessor = ImageProcessor()

def _isAbsolute(url):
    return bool(urlparse.urlparse(url).netloc)

def _getUrlDomain(url):
    return urlparse.urlparse(url).hostname.lower()

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
    parsedHtml = lh.fromstring(rawHtml)

    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True

    return cleaner.clean_html(parsedHtml)

def _extractTextFromElement(element, baseUrl):
    if baseUrl and element.tag == 'a':
        if 'href' in element.attrib:
            baseDomain = _getUrlDomain(baseUrl)
            link = element.attrib['href']
            #don't extract text from foreign links, likely to be ads
            if _isAbsolute(link) and len(link) > 200\
                and baseDomain != _getUrlDomain(link):
                    logger.info(
                        "Filtered out element with link %s while extracting text",
                        link[:50] + "...")
                    return ""

    text_content = ""
    if element.text:
        text_content = element.text

    if len(element):
        for child in element:
            childText = _extractTextFromElement(child, baseUrl)
            if childText:
                text_content += childText + " "

            if child.tail:
                text_content += child.tail + " "

    return text_content.strip()

def _extractText(html, textSelector, baseUrl):
    text = ""
    textDivs = html.cssselect(textSelector)
    for textDiv in textDivs:
        textContent = _extractTextFromElement(textDiv, baseUrl)
        text += textContent + " "
    return text.strip()

def _extractImages(html, imageSelector, baseUrl):
    images = html.cssselect(imageSelector)
    return [_getCompleteUrl(img.attrib['src'], baseUrl) \
        for img in images if 'src' in img.attrib]

def _processImage(jobId, url):
    imageKey = __imageProcessor.processImage(jobId, url)

    if not imageKey:
      return ""
    else:
      return "images/" + imageKey

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
        parsedHtml = _parseAndCleanHtml(rawHtml)
    except (XMLSyntaxError, ParserError):
        logger.info(
            "Could not parse page html. JobId: %s", jobId)
        return ("", [])

    # Extract out text
    text = _extractText(parsedHtml, textSelector, baseUrl)
    if (text == ""):
        logger.info(
            "Did not find any text for selector: %s. JobId: %s",
            textSelector,
            jobId)
    else:
        logger.info(
            "Sucessfully extracted out text of length %i from html. JobId: %s",
            len(text),
            jobId)

    # Extract out images
    images = []
    for imageSelector in imageSelectors:
        images += _extractImages(parsedHtml, imageSelector, baseUrl)
    images = list(set(images)) # remove duplicates
    images = [_processImage(jobId, img) for img in images] #process images
    images = filter(None, images) #filter out unprocessed images
    logger.info("Extracted out %i images. JobId: %s", len(images), jobId)

    return (text, images)

def getSubHtmlEntries(jobId, fullHtml, selector):
    try:
        parsedHtml = _parseAndCleanHtml(fullHtml)
    except (XMLSyntaxError, ParserError):
        logger.warning(
            "Could not parse page html. JobId: %s", jobId)
        return []

    subHtmlEntries = parsedHtml.cssselect(selector)
    return [lh.tostring(entry) for entry in subHtmlEntries]

def extractLink(jobId, html, selector, baseUrl):
    try:
        parsedHtml = _parseAndCleanHtml(html)
    except (XMLSyntaxError, ParserError):
        logger.warning("Could not parse html. JobId: %s", jobId)
        return None

    titleElements = parsedHtml.cssselect(selector)
    if len(titleElements) > 0:
        titleElement = titleElements[0]

        titleText = titleElement.text_content().strip()
        if not titleText:
            return None

        if 'href' in titleElement.attrib:
            return (
                _getCompleteUrl(titleElement.attrib['href'], baseUrl),
                titleText)

    logger.info("Could not extract link. JobId: %s", jobId)
    return None

def extractText(jobId, html, textSelector, baseUrl):
    try:
        parsedHtml = _parseAndCleanHtml(html)
    except (XMLSyntaxError, ParserError):
        logger.warning("Could not parse html. JobId: %s", jobId)
        return None

    # Extract out text
    return _extractText(parsedHtml, textSelector, baseUrl)

def extractOpenGraphData(jobId, rawHtml, baseUrl):
    # Parse html with lxml library
    try:
        # not calling _parseAndCleanHtml as cleaner removes meta tags.
        parsedHtml = lh.fromstring(rawHtml)
    except (XMLSyntaxError, ParserError):
        logger.warning(
            "Could not parse page html. JobId: %s", jobId)
        return { 'images' : [], 'summary' : ""}

    ogImageElements = parsedHtml.cssselect('meta[property="og:image"]')
    ogImages = [_getCompleteUrl(img.attrib['content'], baseUrl) \
        for img in ogImageElements if 'content' in img.attrib]
    ogImages = list(set(ogImages)) # remove duplicates
    ogImages = [_processImage(jobId, img) for img in ogImages] #process images
    ogImages = filter(None, ogImages) #filter out unprocessed images
    logger.info("Extracted out %i og images. JobId: %s", len(ogImages), jobId)

    ogDescElement = parsedHtml.cssselect('meta[property="og:description"]')
    ogDesc = ""
    if (len(ogDescElement) == 1) and ('content' in ogDescElement[0].attrib):
        ogDesc = ogDescElement[0].attrib['content']
        logger.info("Extracted og description. JobId: %s", jobId)

    return { 'images' : ogImages, 'summary' : ogDesc }
