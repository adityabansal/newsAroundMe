def _replaceUnicodeChars(text):
    return (text.
        replace(u'\xc3\xa9', 'e').
        replace(u'\xe2\x80\x90', '-').
        replace(u'\xe2\x80\x91', '-').
        replace(u'\xe2\x80\x92', '-').
        replace(u'\xe2\x80\x93', '-').
        replace(u'\xe2\x80\x94', '-').
        replace(u'\xe2\x80\x94', '-').
        replace(u'\xe2\x80\x98', "'").
        replace(u'\xe2\x80\x99', "'").
        replace(u'\xe2\x80\x9b', "'").
        replace(u'\xe2\x80\x9c', '"').
        replace(u'\xe2\x80\x9c', '"').
        replace(u'\xe2\x80\x9d', '"').
        replace(u'\xe2\x80\x9e', '"').
        replace(u'\xe2\x80\x9f', '"').
        replace(u'\xe2\x80\xb2', "'").
        replace(u'\xe2\x80\xb3', "'").
        replace(u'\xe2\x80\xb4', "'").
        replace(u'\xe2\x80\xb5', "'").
        replace(u'\xe2\x80\xb6', "'").
        replace(u'\xe2\x80\xb7', "'").
        replace(u'\xe2\x81\xba', "+").
        replace(u'\xe2\x81\xbb', "-").
        replace(u'\xe2\x81\xbc', "=").
        replace(u'\xe2\x81\xbd', "(").
        replace(u'\xe2\x81\xbe', ")"))

def removeNonAsciiChars(text):
    text = _replaceUnicodeChars(text)
    return text.encode('ascii', 'ignore').decode('ascii');