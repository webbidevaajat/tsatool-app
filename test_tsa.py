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


    # Block
    def test_Block_init_primary_normal_1(self):
        testinstance = tsa.Block('d2', 'ylojarvi_etelaan_2', 3, 's1122#kitka3_luku >= 0.30')
        instancedict = testinstance.__dict__
        resultdict = {'raw_logic': 's1122#kitka3_luku >= 0.30',
        'master_alias': 'd2',
        'parent_site': 'ylojarvi_etelaan_2',
        'alias': 'd2_3',
        'secondary': False,
        'site': 'ylojarvi_etelaan_2',
        'station': 's1122',
        'source_alias': None,
        'sensor': 'kitka3_luku',
        'operator': '>=',
        'value_str': '0.30'}
        self.assertEqual(instancedict, resultdict)

    def test_Block_init_primary_normal_2(self):
        testinstance = tsa.Block('d2', 'ylojarvi_etelaan_2', 3, 's1122#kitka3_luku in (1, 2, 3)')
        instancedict = testinstance.__dict__
        resultdict = {'raw_logic': 's1122#kitka3_luku in (1, 2, 3)',
        'master_alias': 'd2',
        'parent_site': 'ylojarvi_etelaan_2',
        'alias': 'd2_3',
        'secondary': False,
        'site': 'ylojarvi_etelaan_2',
        'station': 's1122',
        'source_alias': None,
        'sensor': 'kitka3_luku',
        'operator': 'in',
        'value_str': '(1, 2, 3)'}
        self.assertEqual(instancedict, resultdict)

    def test_Block_init_secondary_normal_1(self):
        testinstance = tsa.Block('a1', 'ylojarvi_pohjoiseen_1', 1, 'd1')
        instancedict = testinstance.__dict__
        resultdict = {'raw_logic': 'd1',
        'master_alias': 'a1',
        'parent_site': 'ylojarvi_pohjoiseen_1',
        'alias': 'a1_1',
        'secondary': True,
        'site': 'ylojarvi_pohjoiseen_1',
        'station': None,
        'source_alias': 'd1',
        'sensor': None,
        'operator': None,
        'value_str': None}
        self.assertEqual(instancedict, resultdict)

    def test_Block_init_secondary_normal_2(self):
        testinstance = tsa.Block('d2', 'ylojarvi_etelaan_2', 4, 'ylojarvi_pohjoiseen_1#c3')
        instancedict = testinstance.__dict__
        resultdict = {'raw_logic': 'ylojarvi_pohjoiseen_1#c3',
        'master_alias': 'd2',
        'parent_site': 'ylojarvi_etelaan_2',
        'alias': 'd2_4',
        'secondary': True,
        'site': 'ylojarvi_pohjoiseen_1',
        'station': None,
        'source_alias': 'c3',
        'sensor': None,
        'operator': None,
        'value_str': None}
        self.assertEqual(instancedict, resultdict)

    def test_Block_init_valerr_operator_1(self):
        self.assertRaises(
            ValueError,
            tsa.Block,
            'd2', 'ylojarvi_etelaan_2', 3, 's1122#kitka3_luku in 1, 2, 3')

    def test_Block_init_valerr_nohashtag_1(self):
        self.assertRaises(
            ValueError,
            tsa.Block,
            'd2', 'ylojarvi_etelaan_2', 3, 'd3 > 2')

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
