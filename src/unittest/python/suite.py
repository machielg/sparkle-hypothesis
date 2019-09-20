import unittest

from test1 import Test1
from test2 import Test2

loader = unittest.TestLoader()
suite = unittest.TestSuite()

suite.addTest(loader.loadTestsFromTestCase(Test1))
suite.addTest(loader.loadTestsFromTestCase(Test2))

runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)