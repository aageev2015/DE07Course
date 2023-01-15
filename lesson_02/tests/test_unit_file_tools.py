"""
Unit tests for basic_validators.py
"""
import os
import random
from pathlib import Path
from unittest import TestCase

from support_tools.file_tools import *


class UnitFileToolsTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.__testing_folder_path = \
            f'{Path(__file__).parent}{os.sep}tmp{os.sep}tmp_guarantee_test_{random.randint(100000, 999999)}'
        pass

    @staticmethod
    def __env_path(s: str):
        return s.replace('\\' if os.sep == '/' else '/', os.sep)

    def test_guarantee_folder_exists___when_not_exists_than_created(self):
        if os.path.exists(self.__testing_folder_path):
            os.rmdir(self.__testing_folder_path)

        with_sub_folder = self.__testing_folder_path + os.sep + 'sub'
        guarantee_folder_exists(with_sub_folder)

        self.assertTrue(os.path.exists(with_sub_folder))

        os.rmdir(with_sub_folder)
        os.rmdir(self.__testing_folder_path)

    def test_guarantee_folder_exists___when_exists_than_nothing(self):
        if not os.path.exists(self.__testing_folder_path):
            os.makedirs(self.__testing_folder_path)

        with_sub_folder = self.__testing_folder_path + os.sep + 'sub'
        guarantee_folder_exists(with_sub_folder)

        self.assertTrue(os.path.exists(with_sub_folder))

        os.rmdir(with_sub_folder)
        os.rmdir(self.__testing_folder_path)

    def test_has_path_sub_folder___success_cases(self):
        self.assertTrue(has_path_sub_folder('', 'a'))
        self.assertTrue(has_path_sub_folder('a', 'a/a'))
        self.assertTrue(has_path_sub_folder('b', 'b\\a'))
        self.assertTrue(has_path_sub_folder('c', '/c/a'))
        self.assertTrue(has_path_sub_folder('d', '\\d/a'))
        self.assertTrue(has_path_sub_folder('/a', '/a/a'))
        self.assertTrue(has_path_sub_folder('/b', '/b\\a'))
        self.assertTrue(has_path_sub_folder('/d', '\\d/a'))
        self.assertTrue(has_path_sub_folder('\\a', '/a/a'))
        self.assertTrue(has_path_sub_folder('\\b', '/b\\a'))
        self.assertTrue(has_path_sub_folder('\\d', '\\d/a'))
        self.assertTrue(has_path_sub_folder('\\d', '\\d/a/1/2/3/4/5/6/7/_/-/!'))

    def test_has_path_sub_folder___fail_cases(self):
        self.assertFalse(has_path_sub_folder('a', 'b/a'))
        self.assertFalse(has_path_sub_folder('b', 'b'))
        self.assertFalse(has_path_sub_folder('c', ''))

    def test_remove_leading_slash(self):
        self.assertEqual(remove_leading_slash('/a'), 'a')
        self.assertEqual(remove_leading_slash('\\a'), 'a')
        self.assertEqual(remove_leading_slash('a'), 'a')
        self.assertEqual(remove_leading_slash('/a\\'), 'a\\')
        self.assertEqual(remove_leading_slash('\\a/'), 'a/')
        self.assertEqual(remove_leading_slash('a\\'), 'a\\')
        self.assertEqual(remove_leading_slash('a/'), 'a/')
        self.assertEqual(remove_leading_slash(''), '')
        self.assertEqual(remove_leading_slash('   '), '   ')

    def test_logical_to_physical_file_path(self):
        self.assertEqual(logical_to_physical_file_path('/root1', 'sub1', 'file1.txt'),
                         self.__env_path('/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('/root1/', 'sub1', 'file1.txt'),
                         self.__env_path('/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('/root1', 'sub1/', 'file1.txt'),
                         self.__env_path('/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('/root1/', '/sub1/', 'file1.txt'),
                         self.__env_path('/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('/root1/', 'sub1/', 'file1.txt'),
                         self.__env_path('/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('/root2', '', 'file2.txt'),
                         self.__env_path('/root2/file2.txt'))
        self.assertEqual(logical_to_physical_file_path('/root4', 'sub4', ''),
                         self.__env_path('/root4/sub4/'))
        self.assertEqual(logical_to_physical_file_path('c:\\root1', 'sub1', 'file1.txt'),
                         self.__env_path('c:/root1/sub1/file1.txt'))
        self.assertEqual(logical_to_physical_file_path('root2', 'sub2', 'file2.txt'),
                         self.__env_path('root2/sub2/file2.txt'))
        self.assertEqual(logical_to_physical_file_path('root2\\f', 'sub2\\123', 'file3.txt'),
                         self.__env_path('root2/f/sub2/123/file3.txt'))
        self.assertEqual(logical_to_physical_file_path(
            'c:\\root2\\a\\b\\c\\d\\e\\f\\', '\\1\\2\\3\\4\\5\\6', 'file3_1234567890qwertyuiopasdfghjklzxcbmnm.txt'),
            self.__env_path('c:/root2/a/b/c/d/e/f/1/2/3/4/5/6/file3_1234567890qwertyuiopasdfghjklzxcbmnm.txt'))
