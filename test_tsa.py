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

if __name__ == '__main__':
    unittest.main()