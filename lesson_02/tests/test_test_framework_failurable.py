"""
Test to check that test framework works correctly with failed tests
This failed result exists to check false positive
"""
from unittest import TestCase


class TestCanFail(TestCase):
    def test_when_test_failed_than_failed(self):
        self.assertEqual(True, False)


# TODO
