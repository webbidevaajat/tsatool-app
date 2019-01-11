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


    # eliminate_umlauts()
    def test_eliminate_umlauts_normal(self):
        self.assertEqual(
            tsa.eliminate_umlauts('abcäödefÖÄ123'),
            'abcaodefOA123'
            )


    # to_pg_identifier()
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

    def test_to_pg_identifier_valerr_toolong(self):
        self.assertRaises(
            ValueError,
            tsa.to_pg_identifier,
            'TooLongIdentifierTooLongIdentifierTooLongIdentifierTooLongIdentifier')


    # unpack_logic()
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


    # PrimaryBlock
    def test_PrimaryBlock_init_normal_1(self):
        testinstance = tsa.PrimaryBlock('D2', 3, 's1122#KITKA3_LUKU >= 0.30'
            )
        instancedict = testinstance.__dict__
        resultdict = {'master_alias': 'd2',
        'alias': 'd2_3',
        'station': 's1122',
        'sensor': 'kitka3_luku',
        'operator': '>=',
        'value_str': '0.30'
        }
        self.assertEqual(instancedict, resultdict)

    def test_PrimaryBlock_init_normal_2(self):
        testinstance = tsa.PrimaryBlock(
            'D2', 3, 's1122#KITKA3_LUKU IN (1, 2, 3)'
            )
        instancedict = testinstance.__dict__
        resultdict = {
        'master_alias': 'd2',
        'alias': 'd2_3',
        'station': 's1122',
        'sensor': 'kitka3_luku',
        'operator': 'in',
        'value_str': '(1, 2, 3)'
        }
        self.assertEqual(instancedict, resultdict)


    # SecondaryBlock
    def test_SecondaryBlock_init_normal_1(self):
        pass

    # TODO: Condition?

    # CondCollection
    def test_CondCollection_add_conditions_1(self):
        cc = tsa.CondCollection(
                time_from='2018-01-01 00:00:00',
                time_until='2018-02-01 00:00:00',
                pg_conn=None
            )
        cc.add_condition(
            site='Ylöjärvi_etelään_1',
            master_alias='C4',
            raw_condition='s1122#KITKA3_LUKU >= 0.30 AND s1115#nakyvyys_metria >= 600'
            )
        cc.add_condition(
            site='Ylöjärvi_etelään_1',
            master_alias='C1',
            raw_condition='s1122#KITKA3_LUKU >= 0.40 AND s1115#TIE_1 > 2'
            )

    def test_CondCollection_add_duplicate_aliases_1(self):
        cc = tsa.CondCollection(
                time_from='2018-01-01 00:00:00',
                time_until='2018-02-01 00:00:00',
                pg_conn=None
            )
        cc.add_condition(
            site='Ylöjärvi_etelään_1',
            master_alias='C4',
            raw_condition='s1122#KITKA3_LUKU >= 0.30 AND s1115#nakyvyys_metria >= 600'
            )
        self.assertRaises(
            ValueError,
            cc.add_condition,
            'Ylöjärvi_etelään_1',
            'C4',
            's1122#KITKA3_LUKU >= 0.40 AND s1115#TIE_1 > 2'
            )

    def test_CondCollection_from_dictlist_1(self):
        dl = [
            dict(site='Ylöjärvi_etelään_1',
            master_alias='C4',
            raw_condition='s1122#KITKA3_LUKU >= 0.30 AND s1115#nakyvyys_metria >= 600'),
            dict(site='Ylöjärvi_etelään_1',
            master_alias='C1',
            raw_condition='s1122#KITKA3_LUKU >= 0.40 AND s1115#TIE_1 > 2')
        ]
        tsa.CondCollection.from_dictlist(
            dictlist=dl,
            time_from='2018-01-01 00:00:00',
            time_until='2018-02-01 00:00:00',
            pg_conn=None
            )


if __name__ == '__main__':
    unittest.main()
