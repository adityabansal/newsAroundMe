import logging
import urllib
import urlparse

from lxml.etree import XMLSyntaxError
import lxml.html as lh
from lxml.html.clean import Cleaner

logger = logging.getLogger('htmlProcessor')

def _isAbsolute(url):
    return bool(urlparse.urlparse(url).netloc)

def _getCompleteUrl(url, baseUrl):
    if not baseUrl:
        return url

    if _isAbsolute(url):
        return url
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

def _getImageSize(jobId, url):
    jobInfo = "Image url: " + url + ". JobId: " + jobId

    try:
        logger.info("Fetching image. %s", jobInfo)
        img = urllib.urlopen(url)
        size = img.headers.get("content-length")
        if size:
            logger.info("Image has size %s. %s", size, jobInfo)
            return int(size)
    except:
        pass;

    logger.info("Could not determine image size. %s", jobInfo)
    return 0

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
        logger.warning(
            "Did not find text element for selector: %s. JobId: %s",
            textSelector,
            jobId);
        logger.info(
            "Setting the text as empty. JobId: %s",
            jobId);
        text = "";
    else:
        logger.info(
            "Sucessfully extracted out text from html. JobId: %s",
            jobId);

    # Extract out images
    images = [];
    for imageSelector in imageSelectors:
        images += _extractImages(parsedHtml, imageSelector, baseUrl);
    images = [img for img in images if _getImageSize(jobId, img) > 5000]
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

def extractLink(jobId, html, selector):
    try:
        parsedHtml = _parseAndCleanHtml(html)
    except XMLSyntaxError:
        logger.warning("Could not parse page html. JobId: %s", jobId)
        return None

    titleElements = parsedHtml.cssselect(selector)
    if len(titleElements) > 0:
        titleElement = titleElements[0]
        if 'href' in titleElement.attrib:
            return (titleElement.attrib['href'], titleElement.text_content)

    logger.warning("Could not extract link. JobId: %s", jobId)
    return None