"""
Unit tests for basic_validators.py
"""

from unittest import TestCase

from support_tools.basic_validators import *


class UnitBasicValidatorsTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        pass

    def test_BasicDateValidator___success_cases(self):
        validator = BasicDateValidator()
        self.assertTrue(validator.validate('1980-01-01'))
        self.assertTrue(validator.validate('1980-12-01'))
        self.assertTrue(validator.validate('1980-01-13'))
        self.assertTrue(validator.validate('2000-01-01'))
        self.assertTrue(validator.validate('2000-12-31'))
        self.assertTrue(validator.validate('2022-01-01'))
        self.assertTrue(validator.validate(f'{datetime.MAXYEAR}-01-01'))

    def test_BasicDateValidator___failed_cases(self):
        validator = BasicDateValidator()
        self.assertFalse(validator.validate(''))
        self.assertFalse(validator.validate(' '))
        self.assertFalse(validator.validate('date'))
        self.assertFalse(validator.validate('19800-01-01'))
        self.assertFalse(validator.validate('19800-010-01'))
        self.assertFalse(validator.validate('1980-01-010'))
        self.assertFalse(validator.validate(' 1980-12-01'))
        self.assertFalse(validator.validate('1980-12-01 '))
        self.assertFalse(validator.validate('198O-12-01'))
        self.assertFalse(validator.validate('1980-12-O1'))
        self.assertFalse(validator.validate('1980-12-0i '))
        self.assertFalse(validator.validate('80-12-O1'))
        self.assertFalse(validator.validate('198O-1-01'))
        self.assertFalse(validator.validate('198O-12-1'))

    def test_RelativeFilePathValidator___success_cases(self):
        validator = RelativeFilePathValidator()
        self.assertTrue(validator.validate('1'))
        self.assertTrue(validator.validate('a/'))
        self.assertTrue(validator.validate('/b'))
        self.assertTrue(validator.validate('/c/'))
        self.assertTrue(validator.validate('a\\'))
        self.assertTrue(validator.validate('\\b'))
        self.assertTrue(validator.validate('\\c\\'))
        self.assertTrue(validator.validate('/b\\'))
        self.assertTrue(validator.validate('\\b/'))
        self.assertTrue(validator.validate('e\\2sales_01\\'))
        self.assertTrue(validator.validate('e/2sales_01/'))
        self.assertTrue(validator.validate('\\ff\\s3ales_01'))
        self.assertTrue(validator.validate('gg.txt'))
        self.assertTrue(validator.validate('ee/sal_es/01-02.txt'))
        self.assertTrue(validator.validate('tt/s'))
        self.assertTrue(validator.validate('s-_1234567890qwertyuiopasdfghjklzxcvbnm'))
        self.assertTrue(validator.validate('pp/s_ales\01-02'))
        self.assertTrue(validator.validate('\\ww/sales_\\01-02'))
        self.assertTrue(validator.validate('/rew\\sa__les\\01-02'))
        self.assertTrue(validator.validate('/row/sales/1/2/3/4/5/6/7/8/0/a/b/c/d/e/f/g'))

    def test_RelativeFilePathValidator___failed_cases(self):
        validator = RelativeFilePathValidator()
        self.assertFalse(validator.validate(''))
        self.assertFalse(validator.validate('/'))
        self.assertFalse(validator.validate('\\'))
        self.assertFalse(validator.validate('\\\\raw'))
        self.assertFalse(validator.validate('c:'))
        self.assertFalse(validator.validate('c:\\'))
        self.assertFalse(validator.validate('c:/'))
        self.assertFalse(validator.validate('c:/raw'))
        self.assertFalse(validator.validate('c:/raw/'))
        self.assertFalse(validator.validate('c:\\raw'))
        self.assertFalse(validator.validate('c:\\raw\\'))
        self.assertFalse(validator.validate('/c:\\raw'))

    def test_RawFilePathValidator___success_cases(self):
        validator = RawFilePathValidator()
        self.assertTrue(validator.validate('raw/s'))
        self.assertTrue(validator.validate('raw///s'))
        self.assertTrue(validator.validate('raw\\\\s'))
        self.assertTrue(validator.validate('raw/s/'))
        self.assertTrue(validator.validate('raw\\s'))
        self.assertTrue(validator.validate('/raw/s'))
        self.assertTrue(validator.validate('\\raw\\s'))
        self.assertTrue(validator.validate('/raw\\s'))

    def test_RawFilePathValidator___failed_cases(self):
        validator = RawFilePathValidator()
        self.assertFalse(validator.validate(''))
        self.assertFalse(validator.validate('rew/s'))
        self.assertFalse(validator.validate('raw\\'))
        self.assertFalse(validator.validate('raw\\\\'))
        self.assertFalse(validator.validate('raw/'))
        self.assertFalse(validator.validate('raw//'))
        self.assertFalse(validator.validate('/raw'))
        self.assertFalse(validator.validate('\\raw'))

    def test_StgFilePathValidator___success_cases(self):
        validator = StgFilePathValidator()
        self.assertTrue(validator.validate('stg/s'))
        self.assertTrue(validator.validate('stg///s'))
        self.assertTrue(validator.validate('stg\\\\s'))
        self.assertTrue(validator.validate('stg/s/'))
        self.assertTrue(validator.validate('stg\\s'))
        self.assertTrue(validator.validate('/stg/s'))
        self.assertTrue(validator.validate('\\stg\\s'))
        self.assertTrue(validator.validate('/stg\\s'))

    def test_StgFilePathValidator___failed_cases(self):
        validator = StgFilePathValidator()
        self.assertFalse(validator.validate(''))
        self.assertFalse(validator.validate('ctg/s'))
        self.assertFalse(validator.validate('stg\\'))
        self.assertFalse(validator.validate('stg\\\\'))
        self.assertFalse(validator.validate('stg/'))
        self.assertFalse(validator.validate('stg//'))
        self.assertFalse(validator.validate('/stg'))
        self.assertFalse(validator.validate('\\stg'))
