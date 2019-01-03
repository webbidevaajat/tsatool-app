#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for tsa module.
"""

import unittest
import tsa

class TestTsa(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_eliminate_umlauts_normal(self):
        self.assertEqual(
            tsa.eliminate_umlauts('abcäödefÖÄ123'),
            'abcaodefOA123'
            )

    def test_to_pg_identifier_normal(self):
        self.assertEqual(
            tsa.to_pg_identifier('Ylöjärvi_etelään_2'),
            'ylojarvi_etelaan_2'
            )

    def test_to_pg_identifier_valerr_leading_digit(self):
        self.assertRaises(
            ValueError, 
            tsa.to_pg_identifier, 
            '2_Ylöjärvi_etelään'
            )

    def test_to_pg_identifier_valerr_whitespace(self):
        self.assertRaises(
            ValueError, 
            tsa.to_pg_identifier, 
            'Ylöjärvi etelään'
            )

    def test_unpack_logic_normal(self):
        self.assertEqual(
            tsa.unpack_logic(
                's1122#KITKA3_LUKU >= 0.30'
                ),
            ('s1122', 'kitka3_luku', '>=', '0.30')
            )

    def test_unpack_logic_valerr_1(self):
        self.assertRaises(
            ValueError,
            tsa.unpack_logic,
            's1122#KITKA3_LUKU IN 0.30'
            )

    def test_PrimaryBlock_init_normal_1(self):
        testinstance = tsa.PrimaryBlock(
            'Ylöjärvi_1_etelä', 'D2', 3, 's1122#KITKA3_LUKU >= 0.30'
            )
        instancedict = testinstance.__dict__
        resultdict = {
        'site': 'ylojarvi_1_etela', 
        'master_alias': 'd2', 
        'alias': 'd2_3', 
        'station': 's1122', 
        'sensor': 'kitka3_luku', 
        'operator': '>=', 
        'value_str': '0.30'
        }
        self.assertEqual(instancedict, resultdict)

    def test_PrimaryBlock_init_normal_2(self):
        testinstance = tsa.PrimaryBlock(
            'Ylöjärvi_1_etelä', 'D2', 3, 's1122#KITKA3_LUKU IN (1, 2, 3)'
            )
        instancedict = testinstance.__dict__
        resultdict = {
        'site': 'ylojarvi_1_etela', 
        'master_alias': 'd2', 
        'alias': 'd2_3', 
        'station': 's1122', 
        'sensor': 'kitka3_luku', 
        'operator': 'in', 
        'value_str': '(1, 2, 3)'
        }
        self.assertEqual(instancedict, resultdict)

if __name__ == '__main__':
    unittest.main()