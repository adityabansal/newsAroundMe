import unittest

class SampleTests(unittest.TestCase):

    def testAdd2And2(self):
        self.failUnless((2 + 2 == 4))

if __name__ == '__main__':
    unittest.main()
