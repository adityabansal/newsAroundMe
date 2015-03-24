import string

import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import *

nltk.download('punkt');

def getTokens(text):
    lowers = text.lower()
    #remove the punctuation using the character deletion step of translate
    no_punctuation = lowers.translate(None, string.punctuation)
    tokens = nltk.word_tokenize(no_punctuation)
    return tokens

def getStemmedTokens(text):
    tokens = getTokens(text);
    stemmer = PorterStemmer();
    return [stemmer.stem(token) for token in tokens];    

def getStemmedShingles(text):
    tokens = getStemmedTokens(text);
    twoTokenShingles = \
        [" ".join(tokens[i:i+2]) for i in range(len(tokens) - 1)]
    threeTokenShingles = \
        [" ".join(tokens[i:i+3]) for i in range(len(tokens) - 2)]
    return twoTokenShingles + threeTokenShingles;

def compareTexts(text1, text2):
    text1ShinglesSet = set(getStemmedShingles(text1))
    text2ShinglesSet = set(getStemmedShingles(text2))

    intersection = text1ShinglesSet.intersection(text2ShinglesSet)
    union = text1ShinglesSet.union(text2ShinglesSet)
    return float(len(intersection))/len(union)
