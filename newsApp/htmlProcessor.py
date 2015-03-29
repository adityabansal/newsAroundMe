import logging
import urllib

import lxml.html as lh

logger = logging.getLogger('htmlProcessor')

def _extractText(html, textSelector):
    text = "";
    textDivs = html.cssselect(textSelector);
    for textDiv in textDivs:
        text += textDiv.text_content();
    return text;

def _extractImages(html, imageSelector):
    images = html.cssselect(imageSelector);
    return [img.attrib['src'] for img in images if 'src' in img.attrib];

def processHtml(jobId, rawHtml, textSelector, imageSelectors):
    """
    Process given html to extract out some text and images from it.

    Parameters:
        jobId: a job id to be used for tracing.
        rawHtml: the raw html to be processed.
        textSelector: css selector of element containing text to extract.
        imageSelectors: list of css selectors of elements containing images.
    """

    # Parse html with lxml library
    parsedHtml = lh.fromstring(rawHtml);

    # Extract out text
    text = _extractText(parsedHtml, textSelector)
    if (text == ""):
        logger.warning(
            "Did not find text element for selector: %s. JobId: %s",
            textDiv,
            jobId);
        logger.info(
            "Extracting text from the entire html provided. JobId: %s",
            jobId);
        text = _extractText(parsedHTml, ":not(script)");
    else:
        logger.info(
            "Sucessfully extracted out text from html. JobId: %s",
            jobId);

    # Extract out images
    images = [];
    for imageSelector in imageSelectors:
        images += _extractImages(parsedHtml, imageSelector);
    logger.info("Extracted out %i images. JobId: %s", len(images), jobId);

    return (text, images);
