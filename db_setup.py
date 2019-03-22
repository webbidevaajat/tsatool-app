#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script sets up the db_config.json file for connecting
to the 'tsa' database
and initializes the database, creating a normal user
according to the configuration file.

Before using this script,
you should have installed PostgreSQL and TimescaleDB,
created a superuser and the 'tsa' database owned by that superuser
as well as made sure that you are able to connect to the database
from the machine on which you are going to run this script.
"""

# TODO: read statements from SQL files?
#       Now handling the statements is clumsy
#       and they must be hard-coded in this script
#       in a pretty unreadable way.

import os
import sys
import json
import psycopg2 as pg

from tsa import tsadb_connect

def exit_script():
    print('Quitting database configuration script.')
    raise SystemExit

def prompt_input(txt, yn=False):
    if yn:
        txt += ' [y / n / q]'
    val = None
    val = input(txt).strip()
    if yn:
        while val not in ('y', 'n', 'q'):
            print('Answer must be y, n or q.')
            val = input(txt).strip()
    if val == 'q':
        exit_script()
    else:
        return val

def get_configuration():
    cf_filename = 'db_config.json'
    if os.path.exists(cf_filename):
        print('Configuration file found.')
        try:
            with open(cf_filename, 'r') as cf_file:
                cf = json.load(cf_file)
        except:
            print('Problem reading configuration file.')
            print('Check that the file is a valid JSON file.')
            exit_script()

        # Validate configuration contents
        try:
            assert type(cf['HOST']) is str
            assert type(cf['PORT']) is int
            assert type(cf['DATABASE']) is str
            assert type(cf['ADMIN_USER']) is str
            for u in cf['ORDINARY_USERS']:
                assert type(u) is str
        except Exception as e:
            print(e)
            print('Check configuration file contents.')
            exit_script()
        return cf
    else:
        return None

def exec_statements(cur, statements):
    assert type(statements) is list or type(statements) is tuple
    print('Executing:')
    for s in statements:
        print(s)
        # Prevent printing raw passwords
        #rep_start = s.find('PASSWORD ')
        #if rep_start > 0:
        #    rep_start += 11
        #    rep_end = s.find(';') - 2
        #    rs = s.replace(s[rep_start:rep_end], '*'*(rep_end-rep_start))
        #    print(rs)
        #else:
        #    print(s)
    if prompt_input('OK?', yn=True) == 'y':
        for s in statements:
            cur.execute(s)
        print('Statements executed.\n')
    else:
        print('Statements ignored.\n')
        pass

def main():
    print('TSA DATABASE SETUP')
    print('Type q to any input to exit.')
    print('****************************')
    print()
    conn = None
    try:
        conn = tsadb_connect(username='tsadash')
        if not conn:
            raise Exception('Db connection failed.')
        try:
            with conn.cursor() as cur:

                # SQL STATEMENTS ARE DEFINED HERE!

                # Add extensions for Timescale and exclusion index (see table "obs)"
                exec_statements(cur=cur,
                statements=[
                "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;",
                "CREATE EXTENSION IF NOT EXISTS btree_gist CASCADE;"
                ])

                # Stations table
                exec_statements(cur=cur,
                statements=[
                """CREATE TABLE IF NOT EXISTS stations (
                  id integer PRIMARY KEY,
                  geom json,
                  prop json,
                  modified timestamp DEFAULT NOW()
                );"""
                ])

                # Sensors table
                exec_statements(cur=cur,
                statements=[
                """CREATE TABLE IF NOT EXISTS sensors (
                  id integer PRIMARY KEY,
                  name varchar(40) NOT NULL,
                  shortname varchar(40),
                  unit varchar(40),
                  accuracy integer,
                  nameold varchar(40),
                  valuedescriptions json,
                  description varchar(100),
                  modified timestamp DEFAULT NOW()
                );"""
                ])

                # Station observations ("statobs") table
                exec_statements(cur=cur,
                statements=[
                """CREATE TABLE IF NOT EXISTS statobs (
                  id bigserial NOT NULL,
                  tfrom timestamp NOT NULL,
                  statid integer NOT NULL REFERENCES stations (id),
                  modified timestamp DEFAULT NOW(),
                  PRIMARY KEY (tfrom, statid)
                );""",
                """
                SELECT create_hypertable(
                    'statobs',
                    'tfrom');
                """
                ])

                # Sensor observations ("seobs") table
                # NOTE: "obsid" is not forced to reference
                # statobs (id) (though it would make sense)
                # since this would lead to constraint checks
                # across a huge amount of data.
                # Instead, we rely on inner joins between
                # statobs and seobs when performing analyses.
                exec_statements(cur=cur,
                statements=[
                """CREATE TABLE IF NOT EXISTS seobs (
                  id bigserial PRIMARY KEY,
                  obsid bigint NOT NULL,
                  seid integer NOT NULL,
                  seval real NOT NULL,
                  modified timestamp DEFAULT NOW()
                );""",
                """
                SELECT create_hypertable(
                    'seobs',
                    'id',
                    chunk_time_interval => 10000000
                );
                """,
                """
                CREATE INDEX seobs_obsid_idx
                ON seobs(obsid);
                """,
                """
                CREATE INDEX seobs_seid_seval_idx
                ON seobs(seid, seval);
                """
                ]
                )

                # *************************************** #
                # AD HOC LOTJU RAW DATA TABLES
                # These contain Lotju dump data "as is",
                # just to save it for actual use.
                # Data is to be copied and indexes to be created
                # afterwards.
                exec_statements(cur=cur,
                                statements=[
                """
                CREATE TABLE IF NOT EXISTS tiesaa_mittatieto (
                id bigint NOT NULL,
                aika timestamp NOT NULL,
                asema_id integer NOT NULL,
                PRIMARY KEY (aika, asema_id)
                );
                """,
                """
                SELECT create_hypertable(
                    'tiesaa_mittatieto',
                    'aika'
                );
                """,
                # NOTE: following is defined WITHOUT tiedosto_id
                """
                CREATE TABLE IF NOT EXISTS anturi_arvo (
                id bigint PRIMARY KEY,
                anturi_id integer,
                arvo real,
                mittatieto_id bigint
                );
                """
                """
                SELECT create_hypertable(
                    'anturi_arvo',
                    'id',
                    chunk_time_interval => 10000000
                );
                """,
                """
                CREATE INDEX anturi_arvo_mittatieto_idx
                ON anturi_arvo(mittatieto_id);
                """
                                ]
                                )
                # Tables for old id - new id correspondence
                exec_statements(cur=cur,
                                statements=[
                """
                CREATE TABLE IF NOT EXISTS tiesaa_asema (
                id integer PRIMARY KEY,
                lyhytnimi text,
                vanha_id integer
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS laskennallinen_anturi (
                id integer PRIMARY KEY,
                lyhytnimi text,
                vanha_id integer
                );
                """
                                ])
                # *************************************** #

                # Create triggers that keep the "modified"
                # columns up to date in case existing
                # rows are updated
                exec_statements(cur=cur,
                statements=[
                """DROP FUNCTION IF EXISTS update_modified_column() CASCADE;
                CREATE OR REPLACE FUNCTION update_modified_column()
                RETURNS TRIGGER AS $$
                BEGIN
                  NEW.modified = NOW();
                  RETURN NEW;
                END;
                $$ language 'plpgsql';""",
                """CREATE TRIGGER upd_stations_modified
                  BEFORE UPDATE ON stations
                  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();""",
                """CREATE TRIGGER upd_sensors_modified
                  BEFORE UPDATE ON sensors
                  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();""",
                """CREATE TRIGGER upd_statobs_modified
                  BEFORE UPDATE ON statobs
                  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();""",
                """CREATE TRIGGER upd_seobs_modified
                  BEFORE UPDATE ON seobs
                  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();"""
                ])

                # Create role group for ordinary users,
                # allowing SELECT and temporary CREATE
                # exec_statements(cur=cur,
                # statements=[
                # "DROP ROLE IF EXISTS ordinary_user;",
                # "CREATE ROLE ordinary_user;",
                # "GRANT TEMPORARY ON DATABASE {:s} TO ordinary_user;".format(cf['DATABASE'])
                # ])

                # Create an ordinary user for each in configuration
                # for usrname in cf['ORDINARY_USERS']:
                #     pswd = getpass(prompt='Set password for user {:s}:'.format(usrname))
                #     exec_statements(cur=cur,
                #     statements=[
                #     "DROP ROLE IF EXISTS {:s};".format(usrname),
                #     "CREATE USER {:s};".format(usrname),
                #     "GRANT ordinary_user TO {:s};".format(usrname),
                #     "ALTER USER {:s} PASSWORD '{:s}';".format(usrname, pswd)
                #     ])

                print('All setup statements executed.')

            conn.commit()
            print('All setup statements committed.')

            # Test ordinary user operation
            # if cf['ORDINARY_USERS']:
            #     print('Testing ordinary user operation...')
            #     conn_ord = pg.connect(dbname=cf['DATABASE'],
            #         user=cf['ORDINARY_USERS'][0],
            #         password=getpass(prompt='Password for user {:s}:'.format(cf['ORDINARY_USERS'][0])),
            #         host=cf['HOST'],
            #         port=cf['PORT'])
            #     try:
            #         with conn_ord.cursor() as cur:
            #             cur.execute(
            #             """CREATE TEMPORARY TABLE tt AS (
            #             SELECT * FROM obs LIMIT 10
            #             );""")
            #             cur.execute("SELECT * FROM tt;")
            #         print('Ordinary user operations successfully tested.')
            #     except Exception as e:
            #         print('Could not accomplish ordinary user operation:')
            #         print(e)
            #     finally:
            #         conn_ord.close()

            print('END OF SCRIPT')

        except Exception as e:
            print(e)
            print('Database operations interrupted.')
    except SystemExit:
        sys.exit()
    finally:
        if conn:
            conn.close()
            print('Database connection closed.')

if __name__ == '__main__':
    main()
