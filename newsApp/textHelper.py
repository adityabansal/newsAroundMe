import unicodedata

def removeNonAsciiChars(text):
    return unicodedata.normalize('NFKD', text).encode('ascii','ignore')