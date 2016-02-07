import unittest

from newsApp.translation import translate

class ClusterTableManagerTests(unittest.TestCase):

  def testTranslateHindi(self):
    with open('tests/testData/hindiLong.txt', 'r') as sourceFile:
      hindiText = sourceFile.read()

    result = translate('job', hindiText, 'hi')
    self.assertIsNot(result, '')
