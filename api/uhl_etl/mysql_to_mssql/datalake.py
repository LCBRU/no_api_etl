#!/usr/bin/env python3

import re
import subprocess
import _mssql
import os
import logging
import time
from collections import namedtuple
from tempfile import NamedTemporaryFile
from api.core import Etl, EtlStep, Schedule
from api.environment import (
    ETL_DATABASES_HOST,
    ETL_DATABASES_USERNAME,
    ETL_DATABASES_PASSWORD,
    MS_SQL_DWH_HOST,
    MS_SQL_DWH_USER,
    MS_SQL_DWH_PASSWORD,
)
from concurrent.futures import ThreadPoolExecutor
import itertools
import pandas as pd
from api.database import connection, etl_central_session

SQL_DROP_DB = '''
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{0}')
    DROP DATABASE [{0}];
'''
SQL_CREATE_DB = '''
CREATE DATABASE [{0}];
'''
SQL_SIMPLE_RECOVERY = '''
ALTER DATABASE [{0}] SET RECOVERY SIMPLE;
'''


Ddl = namedtuple('DDL', ['creates', 'indexes', 'foreign_keys'])


def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
        

class MysqlToMssqlStep(EtlStep):

    def __init__(
        self,
        source_database_host,
        source_database_user,
        source_database_password,
        database_name,
        destination_database_host,
        destination_database_user,
        destination_database_password,
        keys_to_ignore=None,
        tables_to_ignore=None,
        constraints_to_ignore=None,
    ):
        super().__init__()
        self.source_database_host = source_database_host
        self.source_database_user = source_database_user
        self.source_database_password = source_database_password
        self.destination_database_host = destination_database_host
        self.destination_database_user = destination_database_user
        self.destination_database_password = destination_database_password

        self.source_database_name = database_name
        self.destination_database_name = 'datalake_{}'.format(database_name)

        self.source_connection_string = 'mysql://{username}:{password}@{host}/{database}'.format(
            username=self.source_database_user,
            password=self.source_database_password,
            host=self.source_database_host,
            database=self.source_database_name,
        )
        self.destination_connection_string = 'mssql+pymssql://{username}:{password}@{host}/{database}'.format(
            username=self.destination_database_user,
            password=self.destination_database_password,
            host=self.destination_database_host,
            database=self.destination_database_name,
        )

        self.keys_to_ignore = keys_to_ignore or []
        self.tables_to_ignore = tables_to_ignore or []
        self.constraints_to_ignore = constraints_to_ignore or []

    def do_etl(self):
        creates_file = NamedTemporaryFile(mode='w+t')
        indexes_file = NamedTemporaryFile(mode='w+t')
        foreign_keys_file = NamedTemporaryFile(mode='w+t')

        try:
            self.dump_ddl(creates_file, indexes_file, foreign_keys_file)
            self.re_create_database(creates_file)
            self.transfer_data_using_inserts()
            self.create_constraints(indexes_file, foreign_keys_file)

        finally:
            creates_file.close()
            indexes_file.close()
            foreign_keys_file.close()

    def re_create_database(self, creates_file):
        self.log("Creating destination database '{}'".format(self.destination_database_name))

        conn = _mssql.connect(
            server=self.destination_database_host,
            user=self.destination_database_user,
            password=self.destination_database_password,
        )

        try:
            conn.execute_non_query(SQL_DROP_DB.format(self.destination_database_name))
            conn.execute_non_query(SQL_CREATE_DB.format(self.destination_database_name))
            conn.execute_non_query(SQL_SIMPLE_RECOVERY.format(self.destination_database_name))

        finally:
            conn.close()

        conn = _mssql.connect(
            server=self.destination_database_host,
            user=self.destination_database_user,
            password=self.destination_database_password,
            database=self.destination_database_name,
        )

        try:
            creates_file.seek(0)
            ddl = creates_file.read()

            self.log(
                message='Recreating database',
                attachment=ddl,
                log_level='INFO',
            )

            conn.execute_non_query(ddl)

        except:
            self.log(
                message='Error running creating database',
                attachment=ddl,
                log_level='ERROR',
            )
            raise

        finally:
            conn.close()

        self.log("Creating destination database '{}' COMPLETED".format(self.destination_database_name))

    def create_constraints(self, indexes_file, foreign_keys_file):
        self.log("Creating constraints for '{}'".format(self.destination_database_name))

        conn = _mssql.connect(
            server=self.destination_database_host,
            user=self.destination_database_user,
            password=self.destination_database_password,
            database=self.destination_database_name,
        )

        try:
            try:
                indexes_file.seek(0)
                ddl = indexes_file.read()
                conn.execute_non_query(ddl)
            except:
                self.log(
                    message='Error running creating indexes',
                    attachment=ddl,
                    log_level='ERROR',
                )
                raise
            try:
                foreign_keys_file.seek(0)
                ddl = foreign_keys_file.read()
                conn.execute_non_query(ddl)
            except:
                self.log(
                    message='Error running creating foreign keys',
                    attachment=ddl,
                    log_level='ERROR',
                )
                raise

        finally:
            conn.close()

        self.log("Creating constraints for '{}' COMPLETED".format(self.destination_database_name))

    def transfer_data_using_pandas(self, tables):

        with connection(self.source_connection_string) as incon, connection(self.destination_connection_string) as outcon:
            for t in tables:
                try:
                    self.log("Extracting '{}'".format(t))
                    for i, df in enumerate(pd.read_sql_table(t, con=incon, chunksize=1000), start=1):
                        self.log("Loading '{}' chunk {}".format(t, i))
                        df.to_sql(t, outcon, if_exists='append', index=False)
                    self.log("Loaded '{}'".format(t))
                except Exception as e:
                    raise Exception('Failed transferring data for table \'{}\''.format(t)) from e

    def transfer_data_using_inserts(self):
        self.log("Dumping Data for '{}'".format(self.source_database_name))

        inserts_file = NamedTemporaryFile(mode='w+t')

        p = subprocess.Popen(
            [
                'mysqldump',
                '--compact',
                '--complete-insert',
                '--no-create-info',
                '--extended-insert=FALSE',
                '--compatible=mssql',
                '--skip-comments',
                '--skip-opt',
                '--skip-set-charset',
                '--skip-triggers',
                '--default-character-set=utf8',
                '--hex-blob',
                '--single-transaction',
                '--quick',
                '-h',
                self.source_database_host,
                '-u',
                self.source_database_user,
                '-p' + self.source_database_password,
                self.source_database_name,
            ],
            stdout=inserts_file,
            universal_newlines=True,
        )

        p.wait()
        inserts_file.flush()
        inserts_file.seek(0)

        conn = _mssql.connect(
            server=self.destination_database_host,
            user=self.destination_database_user,
            password=self.destination_database_password,
            database=self.destination_database_name,
        )

        try:

            BATCH_SIZE = 197
            total_records = 0

            for i, chunk in enumerate(grouper_it(BATCH_SIZE, inserts_file), 1):
                try:
                    inserts = ''.join(chunk)

                    # Remove multiline comments
                    inserts = re.sub(re.compile('/\*(.|[\r\n])*?\*/[;]?', re.MULTILINE), '', inserts)
                    # Remove single line comments
                    inserts = re.sub(re.compile('^--.*$', re.MULTILINE), '', inserts)
                    # Remove DELIMITERS
                    inserts = re.sub(re.compile('^DELIMITER.*$', re.MULTILINE), '', inserts)

                    # Placing all inserts in one transaction,
                    # as opposed to an implicit transaction for
                    # each insert, speeds things up
                    inserts = (
                        'BEGIN TRANSACTION\n' +
                        'SET NOCOUNT ON\n' +
                        inserts +
                        '\nCOMMIT'
                    )

                    # Escape stuff
                    inserts = inserts.replace('\\\\', '{escaped_backslash}')
                    inserts = inserts.replace('\\\'', '\'\'')
                    inserts = inserts.replace('\\%', '%')
                    inserts = inserts.replace('\\_', '_')
                    inserts = inserts.replace('{escaped_backslash}', '\\\\')

                    # MYSQL uses '0000-00-00' for NULL dates
                    inserts = inserts.replace('\'0000-00-00\'', 'NULL')

                    conn.execute_non_query(inserts)

                    if i % 100 == 0:
                        self.log("Approximately {:,} records loaded (batch {})".format(BATCH_SIZE * i, i))
                except:
                    self.log(
                        message='Error loading data',
                        attachment=inserts,
                        log_level='ERROR',
                    )
                    raise
        finally:
            conn.close()
            inserts_file.close()

        self.log("Dumping Data for '{}' COMPLETED".format(self.source_database_name))

    def dump_ddl(self, creates_file, indexes_file, foreign_keys_file):
        self.log("Dumping DDL for '{}'".format(self.source_database_name))

        mysqldump_command = [
            'mysqldump',
            '--compact',
            '-d',
            '--compatible=mssql',
            '--skip-comments',
            '--skip-opt',
            '--skip-set-charset',
            '--skip-triggers',
            '-h',
            self.source_database_host,
            '-u',
            self.source_database_user,
            '-p' + self.source_database_password,
        ]

        mysqldump_command.extend(['--ignore-table={}.{}'.format(self.source_database_name, t) for t in self.tables_to_ignore])

        mysqldump_command.append(self.source_database_name)

        errors_file = NamedTemporaryFile(mode='w+t')

        MyOut = subprocess.Popen(
            mysqldump_command,
            stdout=subprocess.PIPE,
            stderr=errors_file,
            universal_newlines=True,
        )
        tables = []
        current_table_name = ''

        re_comment = re.compile('(/\*.*/\*/|--).*')
        re_comment_start = re.compile('/\*')
        re_comment_end = re.compile('.*\*/.*')
        re_create_table = re.compile('CREATE TABLE (?P<table_name>".*")')
        re_index = re.compile('\s*(?P<unique>UNIQUE|) KEY (?P<key_name>".*") (?P<columns>\(.*\))')
        re_foreign_key = re.compile('\s*CONSTRAINT (?P<constraint_name>".*") FOREIGN KEY (?P<columns>\(.*\)) REFERENCES (?P<references>".*" \(.*\))')

        comment_on = False
        is_comment = False

        ddl = MyOut.stdout.read()

        self.log(
            message='Given DDL',
            attachment=ddl,
            log_level='INFO',
        )

        # Remove multiline comments
        ddl = re.sub(re.compile('/\*(.|[\r\n])*?\*/[;]?', re.MULTILINE), '', ddl)
        # Remove single line comments
        ddl = re.sub(re.compile('^--.*$', re.MULTILINE), '', ddl)

        self.log(
            message='DDL Without comments',
            attachment=ddl,
            log_level='INFO',
        )

        for line in ddl.splitlines():
            # Remove character set
            line = re.sub(r'CHARACTER SET [^\s]+\b', ' ', line)
            line = re.sub(r' COLLATE [^\s]+\b', ' ', line)
            line = re.sub(r' COLLATE utf8_bin', ' COLLATE SQL_Latin1_General_CP1_CS_AS', line)
            line = re.sub(r' COMMENT \'.*\'', '', line)
            line = re.sub(r'SET .*', '', line)
            line = re.sub(r'DELIMITER.*', '', line)

            # Data type conversions
            line = re.sub(r'\bunsigned\b', '', line)
            line = re.sub(r'\bint\(\d+\)', 'INT', line)
            line = re.sub(r'\bsmallint\(\d+\)', 'SMALLINT', line)
            line = re.sub(r'\btinyint\(\d+\)', 'TINYINT', line)
            line = re.sub(r'\bmediumint\(\d+\)', 'INT', line)
            line = re.sub(r'\bbigint\(\d+\)', 'BIGINT', line)
            line = re.sub(r'\bdouble\b', ' FLOAT(53)', line)
            line = re.sub(r'\bbit\(\d+\)', 'BIT', line)

            line = re.sub(r'\btinyblob\b', 'varbinary(max)', line)
            line = re.sub(r'\bmediumblob\b', 'varbinary(max)', line)
            line = re.sub(r'\blongblob\b', 'varbinary(max)', line)
            
            line = re.sub(r'\bblob\b', 'varchar(max)', line)

            line = re.sub(r'\btinytext\b', 'varchar(max)', line)
            line = re.sub(r'\bmediumtext\b', 'varchar(max)', line)
            line = re.sub(r'\blongtext\b', 'varchar(max)', line)
            line = re.sub(r'\btext\(\d+\)\b', 'varchar(max)', line)
            line = re.sub(r'\btext\b', 'varchar(max)', line)

            # datetime2 has a range that matches MySQL
            line = re.sub(r'\bdatetime\b', 'datetime2', line)
            line = re.sub(r'[^"]\btimestamp\b', 'datetime2', line)

            # Convert ENUMS to VARCHARS(255)
            line = re.sub(r'enum\(.*\)', 'varchar(255)', line)

            line = re.sub(r'\bDEFAULT b\'', 'DEFAULT \'', line)

            table_name_match = re.match(re_create_table, line)

            if table_name_match:
                current_table_name = table_name_match.group('table_name')
                tables.append(current_table_name.strip("\""))

            index_match = re.match(re_index, line)
            foreign_key_match = re.match(re_foreign_key, line)

            if index_match:
                if not index_match.group('key_name').replace('"', '') in self.keys_to_ignore:
                    columns = re.sub(r'\(\d+\)', '', index_match.group('columns')) # Delete strange number in brackets
                    indexes_file.write(
                        'CREATE {} INDEX {} ON {} {};\n'.format(
                            index_match.group('unique'),
                            index_match.group('key_name'),
                            current_table_name,
                            columns,
                        )
                    )
            elif foreign_key_match:
                if not foreign_key_match.group('constraint_name') in self.constraints_to_ignore:
                    foreign_keys_file.write(
                        'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY {} REFERENCES {};\n'.format(
                            current_table_name,
                            foreign_key_match.group('constraint_name'),
                            foreign_key_match.group('columns'),
                            foreign_key_match.group('references'),
                        )
                    )
            else:
                if line:
                    creates_file.write(line + '\n')
        
        errors_file.flush()
        errors_file.seek(0)
        for e in errors_file:
            self.log(e, log_level='ERROR')
        errors_file.close()

        self.log("Dumping DDL for '{}' COMPLETED".format(self.source_database_name))
        return tables


class DataLakeStep(MysqlToMssqlStep):

    def __init__(
        self,
        database_name,
        keys_to_ignore=None,
        tables_to_ignore=None,
        constraints_to_ignore=None,
    ):
        super().__init__(
            source_database_host=ETL_DATABASES_HOST,
            source_database_user=ETL_DATABASES_USERNAME,
            source_database_password=ETL_DATABASES_PASSWORD,
            database_name=database_name,
            destination_database_host=MS_SQL_DWH_HOST,
            destination_database_user=MS_SQL_DWH_USER,
            destination_database_password=MS_SQL_DWH_PASSWORD,
            keys_to_ignore=keys_to_ignore,
            tables_to_ignore=tables_to_ignore,
            constraints_to_ignore=constraints_to_ignore,
        )


class DataLake_BriccsStep(DataLakeStep):
    def __init__(self):
        super().__init__(database_name='briccs')


class DataLake_BriccsNorthamtonStep(DataLakeStep):
    def __init__(self):
        super().__init__(database_name='briccs_northampton')


class DataLake_CivicrmStep(DataLakeStep):
    def __init__(self):
        super().__init__(
            database_name='civicrmlive_docker4716',
            keys_to_ignore=[
                '"index_image_URL_128"',
                '"UI_external_identifier"',
            ]
        )


class DataLake_IdentityStep(DataLakeStep):
    def __init__(self):
        super().__init__(
            database_name='identity',
            keys_to_ignore=[
                'uix_bioresource_id_bioresource_id_provider_id_legacy_number',
                'ix_demographics_request_column_definition_dob_column_id',
                'ix_demographics_request_column_definition_family_name_column_id',
                'ix_demographics_request_column_definition_gender_column_id',
                'ix_demographics_request_column_definition_given_name_column_id',
                'ix_demographics_request_column_definition_nhs_number_column_id',
                'ix_demographics_request_column_definition_postcode_column_id',
            ]
        )


class DataLake_GenvascGpPortalStep(DataLakeStep):
    def __init__(self):
        super().__init__(
            database_name='genvasc_gp_portal',
            keys_to_ignore=[
                '"idx_recruit_civicrm_case_id"',
            ],
        )


class DataLake_RedCapStep(DataLakeStep):
    def __init__(self, database_name):
        super().__init__(
            database_name=database_name,
            keys_to_ignore=[
                '"password_reset_key"',
                '"nonrule_proj_record_event_field"',
                '"pd_rule_proj_record_event_field"',
                '"rule_record_event"',
                '"map_id_mr_id_timestamp_value"',
                '"project_id_comment"',
                '"log_view_id"',
                '"log_view_id_time"',
                '"twilio_from_number"',
                '"project_note"',
                '"logo"',
                '"email_verify_code"',
                '"email2_verify_code"',
                '"email3_verify_code"',
                '"api_token"',
                '"user_comments"',
                '"project_field_prid"',
                '"legacy_hash"',
                '"access_code"',
                '"access_code_numeral"',
                '"ss_id_record"',
            ]
        )


class DataLake_RedCapBriccsStep(DataLake_RedCapStep):
    def __init__(self):
        super().__init__(database_name='redcap6170_briccs')


class DataLake_RedCapBriccsExtStep(DataLake_RedCapStep):
    def __init__(self):
        super().__init__(database_name='redcap6170_briccsext')


class DataLake_RedCapBriccsUoLCrfStep(DataLake_RedCapStep):
    def __init__(self):
        super().__init__(database_name='uol_crf_redcap')


class DataLake_RedCapBriccsUoLSurveyStep(DataLake_RedCapStep):
    def __init__(self):
        super().__init__(database_name='uol_survey_redcap')


class DataLake_OpenSpecimenStep(DataLakeStep):
    def __init__(self):
        super().__init__(
            database_name='uol_openspecimen',
            keys_to_ignore=[
                '"barcode"',
                '"cat_cpr_ext_subj_id_uq"',
                '"os_cp_code_uq"',
                '"social_security_number"',
                '"catissue_site_code_uq"',
                '"cat_spec_cp_id_label_uq"',
                '"cat_spec_cp_id_barcode_uq"',
                '"name"',
            ],
            tables_to_ignore=[
                'os_container_hierarchy_view',
                'os_cpr_spmn_stats_view',
                'os_cpr_visit_stats_view',
                'os_storage_cont_stats_view',
            ],
            constraints_to_ignore=[
                '"fk_cat_scst_scid_container_ids"',
                '"fkc1a3c8cc7f0c2c7"',
                '"fk49b8de5dac76c0"',
                '"fk_cat_cc_cid_cat_sc_id"',
                '"fk_cat_csr_roleid_cat_role_id"',
                '"fk_distri_distri_sp_event"',
                '"fk703b902159a3ce5c"',
                '"fk28429d0159a3ce5c"',
                '"fk847da57775255ca5"',
                '"fk_querytag_obj_id"',
                '"fk_dyoqily0s2m061bxvxik0wou5"',
                '"fk_cx3wxvecf9prg0ulwaw3bwb2s"',
                '"fk_spec_coll_event"',
                '"fk_9xhryw88q1uv2s2oxcn53rsdh"',
                '"fk_4rwywid6his5vawygbbok91h7"',
                '"fk_r51pe9nboapngjjh34hbm3i2s"',
                '"fk_c4n30y077ycht0hl58ri6fko7"',
                '"fk_pkjkhvgsgjkgpwjrg65viug05"',
                '"fk_hs8crr2d3nwca17qqfli82ym"',
                '"fk_qaypykblepjftrcv2xe056rvh"',
                '"fk_spec_rec_event"',
                '"fk_isb8mltbk8qaa3q4epkoy9ell"',
                '"fk_w3jps5e5byvocjpg18nuyffy"',
                '"fk_8bytnb90kr6dd9hr194p9ei57"',
                '"fk_ctda38ykfjtwuljy7pb6y753d"',
                '"fk_m9slxw75ecrvrk31nwwamd7ng"',
                '"fk_auth_tokens_la_log_id"',
                '"fk_cfg_props_module_id"',
                '"fk_7nmb9yn4uq0fojtbh2nl7stv2"',
            ]
        )


class CombinedDataLakeEtl(Etl):

    def __init__(
        self,
    ):
        super().__init__(schedule=Schedule.daily)

    def do_etl(self):
        with ThreadPoolExecutor(max_workers = 4) as executor:

            for step_class in [
                DataLake_RedCapBriccsStep,
                DataLake_OpenSpecimenStep,
                DataLake_BriccsStep,
                DataLake_BriccsNorthamtonStep,
                DataLake_CivicrmStep,
                DataLake_IdentityStep,
                DataLake_GenvascGpPortalStep,
                DataLake_RedCapBriccsExtStep,
                DataLake_RedCapBriccsUoLCrfStep,
                DataLake_RedCapBriccsUoLSurveyStep,
            ]:
                step = step_class()
                executor.submit(step.run)
