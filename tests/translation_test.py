import unittest

from newsApp.translation import translate

class ClusterTableManagerTests(unittest.TestCase):

  def testTranslateHindi(self):
    with open('tests/testData/hindiLong.txt', 'r') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'hi')
    self.assertIsNot(result, '')

  def testTranslateBengali(self):
    with open('tests/testData/bengaliLong.txt', 'r') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'bn')
    self.assertIsNot(result, '')

  def testTranslateTamil(self):
    with open('tests/testData/tamilLong.txt', 'r') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'ta')
    self.assertIsNot(result, '')

  def testTranslateMarathi(self):
    with open('tests/testData/marathiLong.txt', 'r') as sourceFile:
      text = sourceFile.read()

    result = translate('job', text, 'mr')
    self.assertIsNot(result, '')
