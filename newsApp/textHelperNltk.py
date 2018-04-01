import itertools
import logging
import string

from retrying import retry
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import *

from encodedEntity import EncodedEntity
from textHelper import removeNonAsciiChars

nltk.download('punkt');
nltk.download('stopwords');
nltk.download('maxent_treebank_pos_tagger');
nltk.download('maxent_ne_chunker');
nltk.download('averaged_perceptron_tagger')
nltk.download('words')

def _removePuntuation(text):
    if isinstance(text, str):
        return text.translate(None, string.punctuation)
    elif isinstance(text, unicode):
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        return text.translate(remove_punctuation_map)

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def getSentences(text):
    return nltk.sent_tokenize(text)

def getDigitForToken(token):
    digitsDict = {
        'one': '1',
        'two': '2',
        'three': '3',
        'four': '4',
        'five': '5',
        'six': '6',
        'seven': '7',
        'eight': '8',
        'nine': '9',
        'ten': '10',
        'eleven': '11',
        'twelve': '12',
        'thirteen': '13',
        'fourteen': '14',
        'fifteen': '15',
        'sixteen': '16',
        'seventeen': '17',
        'eighteen': '18',
        'nineteen': '19'
    }

    return digitsDict.get(token, token)

def replaceWordTokensWithDigits(tokens):
    return [getDigitForToken(token) for token in tokens]

def getTokens(text):
    text = removeNonAsciiChars(text);
    lowers = text.lower()
    no_punctuation = _removePuntuation(lowers)
    tokens = nltk.word_tokenize(no_punctuation)
    tokens = replaceWordTokensWithDigits(tokens)
    return tokens

def getStemmedTokens(text):
    tokens = getTokens(text);
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

def compareEnglishTexts(text1, text2):
    text1ShinglesSet = set(getStemmedShingles(text1, 3, 3))
    text2ShinglesSet = set(getStemmedShingles(text2, 3, 3))

    intersection = text1ShinglesSet.intersection(text2ShinglesSet)
    shorterLen = min(len(text1ShinglesSet), len(text2ShinglesSet))

    if shorterLen == 0:
        return 0
    else:
        return float(len(intersection))/shorterLen

def compareEnglishTitles(title1, title2):
    title1Tokens = set(filterStopwords(getStemmedTokens(title1)))
    title2Tokens = set(filterStopwords(getStemmedTokens(title2)))

    intersection = title1Tokens.intersection(title2Tokens)
    shorterLen = min(len(title1Tokens), len(title2Tokens))

    if  shorterLen == 0:
        return 0
    else:
        return float(len(intersection))/shorterLen

@retry(stop_max_attempt_number=3)
def getEntitiesInternal(text):
    if not text:
        return []

    text = removeNonAsciiChars(text)

    sentences = nltk.sent_tokenize(text)
    sentences = [nltk.word_tokenize(sent) for sent in sentences]
    sentences = [nltk.pos_tag(sent) for sent in sentences]

    entities = [];
    for sentence in sentences:
        extractedEntities = nltk.ne_chunk(sentence, binary=True).subtrees(
            filter = lambda x: x.label() == 'NE')
        for entity in extractedEntities:
            newEntity = ' '.join([leaf[0] for leaf in entity.leaves()])
            entities.append(newEntity)

    return list(set(entities))

def getEntities(text):
    try:
        return getEntitiesInternal(text)
    except Exception as e:
        logging.info("Could not extract entities for text: '%s'", text)
        return []

def compareEntities(entity1, entity2, doc1EntityWeights, doc2EntityWeights):
    entity1 = EncodedEntity(entity1)
    entity1Weigth = doc1EntityWeights.get(entity1, 0.8)
    entity2 = EncodedEntity(entity2)
    entity2Weigth = doc2EntityWeights.get(entity2, 0.8)
    combinedWeight = entity1Weigth * entity2Weigth;

    if entity1.encoded == entity2.encoded:
        return 1.0 * combinedWeight
    else:
        entity1Words = set(entity1.encoded.split())
        entity2Words = set(entity2.encoded.split())
        commonWords = entity1Words.intersection(entity2Words)

        if len(commonWords) > 0:
            return combinedWeight * float(len(commonWords))/(len(entity1Words) + len(entity2Words))
        else:
            return 0.0

def compareTextEntities(text1, text2, doc1EntityWeights, doc2EntityWeights):
    text1Entities = set(getEntities(text1))
    text2Entities = set(getEntities(text2))

    entityPairSimilarities = [compareEntities(x[0], x[1], doc1EntityWeights, doc2EntityWeights)
                 for x in itertools.product(text1Entities, text2Entities)]

    if len(entityPairSimilarities) == 0:
        return 0;
    else:
        lessNumberOfEntities = min(len(text1Entities), len(text2Entities))
        score = float(sum(entityPairSimilarities))/(max(lessNumberOfEntities, 2))
        if (score >= 1.0):
            return 1.0
        else:
            return score

def getImportantSentences(text):
    sentences = getSentences(text);
    importantSentences= [];

    for sentence in sentences:
        nEntities = len(getEntities(sentence))
        if nEntities > 3 and nEntities < 6:
            importantSentences.append(sentence)
        elif nEntities == 3:
            if " said " in sentence or " told " in sentence or hasNumbers(sentence):
                importantSentences.append(sentence)

    return importantSentences;
