#!/usr/bin/env python3

import re
import subprocess
from api.core import Etl, Schedule
from api.environment import (
    ETL_DATABASES_HOST,
    ETL_DATABASES_USERNAME,
    ETL_DATABASES_PASSWORD,
)


class MysqlToMssqlEtl(Etl):

    def __init__(
        self,
        schedule,
        source_database_host,
        source_database_user,
        source_database_password,
        source_database_name,
    ):
        super().__init__(schedule=schedule)
        self.source_database_host = source_database_host
        self.source_database_user = source_database_user
        self.source_database_password = source_database_password
        self.source_database_name = source_database_name

    def do_etl(self):
        creates, indexes, foreign_keys = self.dump_dll()

    def dump_dll(self):
        MyOut = subprocess.Popen(
            [
                'mysqldump',
                '--compact',
                '-d',
                '--compatible=mssql',
                '-h',
                self.source_database_host,
                '-u',
                self.source_database_user,
                '-p' + self.source_database_password,
                self.source_database_name,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        ddl, stderr = MyOut.communicate()
        ddl = ddl.decode('utf8')

        # Remove comments
        ddl = re.sub(r'(?m)^(/\*|--).*\n?', '', ddl)

        # Remove character set
        ddl = re.sub(r'CHARACTER SET utf8 ', '', ddl)

        # Convert 'int(11)' to 'INT'
        ddl = re.sub(r'int\(\d+\)', 'INT', ddl)

        indexes = []
        foreign_keys = []
        creates = []
        table_name = ''

        re_create_table = re.compile('CREATE TABLE (?P<table_name>".*")')
        re_index = re.compile('\s*(?P<unique>UNIQUE|) KEY (?P<key_name>".*") (?P<columns>\(.*\))')
        re_foreign_key = re.compile('\s*CONSTRAINT (?P<constraint_name>".*") FOREIGN KEY (?P<columns>\(.*\)) REFERENCES (?P<references>".*" \(.*\))')

        for l in ddl.splitlines():
            table_name_match = re.match(re_create_table, l)

            if table_name_match:
                table_name = table_name_match.group('table_name')

            index_match = re.match(re_index, l)
            foreign_key_match = re.match(re_foreign_key, l)

            if index_match:
                indexes.append(
                    'CREATE {} INDEX {} ON {} {};'.format(
                        index_match.group('unique'),
                        index_match.group('key_name'),
                        table_name,
                        index_match.group('columns'),
                    )
                )
            elif foreign_key_match:
                foreign_keys.append(
                    'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY {} REFERENCES {};'.format(
                        table_name,
                        foreign_key_match.group('constraint_name'),
                        foreign_key_match.group('columns'),
                        foreign_key_match.group('references'),
                    )
                )
            else:
                creates.append(l)

        return '\n'.join(creates), '\n'.join(indexes), '\n'.join(foreign_keys)


class IdentityEtl(MysqlToMssqlEtl):

    def __init__(self):
        super().__init__(
            schedule=Schedule.daily,
            source_database_host=ETL_DATABASES_HOST,
            source_database_user=ETL_DATABASES_USERNAME,
            source_database_password=ETL_DATABASES_PASSWORD,
            source_database_name='identity',
        )
