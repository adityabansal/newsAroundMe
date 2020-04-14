import unittest

from newsApp.translation import translate, translateMicrosoft, translateGoogle

class ClusterTableManagerTests(unittest.TestCase):

  def testTranslateHindi(self):
    with open('tests/testData/hindiLong.txt', 'r', encoding='utf8') as sourceFile:
      text = sourceFile.read()

    result = translateMicrosoft('job', text, 'hi')
    self.assertTrue(isinstance(result, str))
    self.assertIsNot(result, '')

  def testTranslateBengali(self):
    with open('tests/testData/bengaliLong.txt', 'r', encoding='utf8') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'bn')
    self.assertTrue(isinstance(result, str))
    self.assertIsNot(result, '')

  def testTranslateTamil(self):
    with open('tests/testData/tamilLong.txt', 'r', encoding='utf8') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'ta')
    self.assertTrue(isinstance(result, str))
    self.assertIsNot(result, '')

  def testTranslateMarathi(self):
    with open('tests/testData/marathiLong.txt', 'r', encoding='utf8') as sourceFile:
      text = sourceFile.read()

    result = translateGoogle('job', text, 'mr')
    self.assertTrue(isinstance(result, str))
    self.assertIsNot(result, '')
