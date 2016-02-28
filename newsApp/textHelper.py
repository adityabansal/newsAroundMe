import logging
import string

import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import *

nltk.download('punkt');
nltk.download('stopwords');
nltk.download('maxent_treebank_pos_tagger');
nltk.download('maxent_ne_chunker');
nltk.download('words')

def _removePuntuation(text):
    if isinstance(text, str):
        return text.translate(None, string.punctuation)
    elif isinstance(text, unicode):
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        return text.translate(remove_punctuation_map)

def _removeNonAsciiChars(text):
    return "".join([ch for ch in text if ord(ch)<= 128]);

def getTokens(text):
    lowers = text.lower()
    no_punctuation = _removePuntuation(lowers)
    tokens = nltk.word_tokenize(no_punctuation)
    return tokens

def getStemmedTokens(text):
    safe_text = _removeNonAsciiChars(text);
    tokens = getTokens(safe_text);
    stemmer = PorterStemmer();
    return [stemmer.stem(token) for token in tokens];    

def filterStopwords(tokens):
    return [t for t in tokens if not t in stopwords.words('english')];

def getStemmedShingles(text, minLength, maxLength):
    if text is None:
        return [];

    tokens = filterStopwords(getStemmedTokens(text));
    shingles = [];

    for length in range(minLength, maxLength + 1):
        shingles = shingles + \
            [" ".join(tokens[i:i+length]) for i in range(len(tokens) - length + 1)]

    return shingles;

def compareTexts(text1, text2):
    text1ShinglesSet = set(getStemmedShingles(text1, 3, 3))
    text2ShinglesSet = set(getStemmedShingles(text2, 3, 3))

    intersection = text1ShinglesSet.intersection(text2ShinglesSet)
    shorterLen = min(len(text1ShinglesSet), len(text2ShinglesSet))

    if shorterLen == 0:
        return 0
    else:
        return float(len(intersection))/shorterLen

def compareTitles(title1, title2):
    title1Tokens = set(filterStopwords(getStemmedTokens(title1)))
    title2Tokens = set(filterStopwords(getStemmedTokens(title2)))

    intersection = title1Tokens.intersection(title2Tokens)
    shorterLen = min(len(title1Tokens), len(title2Tokens))

    if  shorterLen == 0:
        return 0
    else:
        return float(len(intersection))/shorterLen

def getEntities(text):
    try:
        text = _removeNonAsciiChars(text)

        sentences = nltk.sent_tokenize(text)
        sentences = [nltk.word_tokenize(sent) for sent in sentences]
        sentences = [nltk.pos_tag(sent) for sent in sentences]

        entities = [];
        for sentence in sentences:
            extractedEntities = nltk.ne_chunk(sentence, binary=True).subtrees(
                filter = lambda x: x.label() == 'NE')
            for entity in extractedEntities:
                entities.append(
                    ' '.join([leaf[0] for leaf in entity.leaves()]))

        return list(set(entities))
    except Exception as e:
        logging.exception("Could not extract entities for text: '%s'", text)
        return []