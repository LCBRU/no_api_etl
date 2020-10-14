#!/usr/bin/env python3

import fileinput
import requests
import shutil
import gzip
import tempfile
import subprocess
import re
from urllib import parse
from api.core import Etl, Schedule
from api.environment import (
    ETL_DATABASES_HOST,
    ETL_DATABASES_USERNAME,
    ETL_DATABASES_PASSWORD,
    ETL_DOWNLOAD_USERNAME,
    ETL_DOWNLOAD_PASSWORD,
    ETL_ENCRYPTION_PASSWORD,
)


class DatabaseDownloadAndRestore(Etl):

    def __init__(
        self,
        schedule,
        url,
        url_username,
        url_password,
        decrypt_password,
        destination_database_name,
        database_host,
        database_user,
        database_password,
        lowercase=False,
    ):
        super().__init__(schedule=schedule)
        self.url = url
        self.url_username = url_username
        self.url_password = url_password
        self.decrypt_password = decrypt_password
        self.destination_database_name = destination_database_name
        self.database_host = database_host
        self.database_user = database_user
        self.database_password = database_password
        self.lowercase = lowercase

    def do_etl(self):
        downloaded_filename = tempfile.NamedTemporaryFile()
        decrypted_filename = tempfile.NamedTemporaryFile()
        unzipped_filename = tempfile.NamedTemporaryFile()

        self.download_file(
            url=self.url,
            output_filename=downloaded_filename.name,
            username=self.url_username,
            password=self.url_password,
            )

        self.decrypt_file(
            input_filename=downloaded_filename.name,
            output_filename=decrypted_filename.name,
            password=self.decrypt_password,
        )

        self.unzip_file(
            input_filename=decrypted_filename.name,
            output_filename=unzipped_filename.name,
        )

        self.amend_database_name(
            input_filename=unzipped_filename.name,
        )

        self.drop_database()
        self.create_database()
        self.restore_database(unzipped_filename.name)

        downloaded_filename.close()
        decrypted_filename.close()
        unzipped_filename.close()

    def download_file(self, url, output_filename, username, password):
        with requests.get(url, stream=True, auth=(username, password)) as r:
            with open(output_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

    def decrypt_file(self, input_filename, output_filename, password):
        proc = subprocess.run([
            'gpg',
            '--decrypt',
            '--batch',
            '--output',
            output_filename,
            '--passphrase',
            password,
            '--no-use-agent',
            '--yes',
            input_filename,
        ])

        if proc.returncode != 0:
            raise Exception('Could not decrypt file error code = {}.'.format(proc.returncode))

    def unzip_file(self, input_filename, output_filename):
        with gzip.open(input_filename, 'rb') as f_in:
            with open(output_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    def amend_database_name(self, input_filename):
        create_db = re.compile('create\s*database', re.IGNORECASE)
        use_db = re.compile('use\s', re.IGNORECASE)

        with fileinput.FileInput(input_filename, inplace=True) as file:
            for line in file:
                if not create_db.match(line) and not use_db.match(line):
                    processed = line
                    if self.lowercase:
                        processed = processed.lower()
                    print(processed, end='')

    def create_database(self):
        proc = self.run_mysql('CREATE DATABASE {};'.format(self.destination_database_name))

        if proc.returncode != 0:
            raise Exception('Could not create database "{}" (ERROR: {})'.format(
                self.destination_database_name,
                proc.returncode,
            ))

    def drop_database(self):
        proc = self.run_mysql('DROP DATABASE IF EXISTS {};'.format(self.destination_database_name))

        if proc.returncode != 0:
            raise Exception('Could not drop database "{}" (ERROR: {})'.format(
                self.destination_database_name,
                proc.returncode,
                ))

    def run_mysql(self, command):
        return subprocess.run([
            'mysql',
            '-h',
            self.database_host,
            '-u',
            self.database_user,
            '--password={}'.format(self.database_password),
            '-e',
            command,
        ])

    def restore_database(self, input_filename):
        proc = self.run_mysql('USE {};\nSOURCE {}'.format(
            self.destination_database_name,
            input_filename,
        ))

        if proc.returncode != 0:
            raise Exception('Could not restore database "{}" (ERROR: {})'.format(
                self.destination_database_name,
                proc.returncode,
            ))


class OpenSpecimenDatabase(DatabaseDownloadAndRestore):

    def __init__(self):
        super().__init__(
            schedule=Schedule.daily_6pm,
            url='https://catissue-live.lcbru.le.ac.uk/publish/catissue.db',
            url_username=ETL_DOWNLOAD_USERNAME,
            url_password=ETL_DOWNLOAD_PASSWORD,
            decrypt_password=ETL_ENCRYPTION_PASSWORD,
            destination_database_name='uol_openspecimen',
            database_host=ETL_DATABASES_HOST,
            database_user=ETL_DATABASES_USERNAME,
            database_password=ETL_DATABASES_PASSWORD,
            lowercase=True,
        )


class UoLRedcapCrfDatabase(DatabaseDownloadAndRestore):

    def __init__(self):
        super().__init__(
            schedule=Schedule.daily_6pm,
            url='https://crf.lcbru.le.ac.uk/publish/redcap.db',
            url_username=ETL_DOWNLOAD_USERNAME,
            url_password=ETL_DOWNLOAD_PASSWORD,
            decrypt_password=ETL_ENCRYPTION_PASSWORD,
            destination_database_name='uol_crf_redcap',
            database_host=ETL_DATABASES_HOST,
            database_user=ETL_DATABASES_USERNAME,
            database_password=ETL_DATABASES_PASSWORD,
            lowercase=False,
        )


class UoLRedcapSurveyDatabase(DatabaseDownloadAndRestore):

    def __init__(self):
        super().__init__(
            schedule=Schedule.daily_6pm,
            url='https://redcap.lcbru.le.ac.uk/publish/redcap.db',
            url_username=ETL_DOWNLOAD_USERNAME,
            url_password=ETL_DOWNLOAD_PASSWORD,
            decrypt_password=ETL_ENCRYPTION_PASSWORD,
            destination_database_name='uol_survey_redcap',
            database_host=ETL_DATABASES_HOST,
            database_user=ETL_DATABASES_USERNAME,
            database_password=ETL_DATABASES_PASSWORD,
            lowercase=False,
        )


class UoLRedcapEasyAsDatabase(DatabaseDownloadAndRestore):

    def __init__(self):
        super().__init__(
            schedule=Schedule.daily_6pm,
            url='https://easy-as.lbrc.le.ac.uk/publish/redcap.db',
            url_username=ETL_DOWNLOAD_USERNAME,
            url_password=ETL_DOWNLOAD_PASSWORD,
            decrypt_password=ETL_ENCRYPTION_PASSWORD,
            destination_database_name='uol_easyas_redcap',
            database_host=ETL_DATABASES_HOST,
            database_user=ETL_DATABASES_USERNAME,
            database_password=ETL_DATABASES_PASSWORD,
            lowercase=False,
        )
