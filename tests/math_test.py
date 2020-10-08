import unittest

from pjbmath import double_it


class TestMaths(unittest.TestCase):
    def test_something(self):
        self.assertEqual(8, double_it(4))

if __name__ == '__main__':
    unittest.main()
