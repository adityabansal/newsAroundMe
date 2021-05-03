import unittest
import time

from newsApp.dbhelper import decryptSecret, encryptSecret

class DbHelperTests(unittest.TestCase):

    def testEncryptDecrypt(self):
        encKey = 'kjvorn#4gjha52sg'
        value = 'testSecretValue'

        encryptedVal = encryptSecret(value, encKey)

        decryptedVal = decryptSecret(encryptedVal, encKey)
        self.assertEqual(decryptedVal, value)

