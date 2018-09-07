#!/usr/bin/env python3

import requests
import shutil
import gzip
import tempfile
import subprocess
from urllib import parse
from api.core import Etl, Schedule


class OpenSpecimenDatabase(Etl):

    def __init__(self):
        super().__init__(schedule=Schedule.never)

    def do_etl(self):
        downloaded_filename = tempfile.NamedTemporaryFile()
        decrypted_filename = tempfile.NamedTemporaryFile()
        unzipped_filename = tempfile.NamedTemporaryFile()

        self.download_file(
            url='https://catissue-live.lcbru.le.ac.uk/publish/catissue.db',
            output_filename=downloaded_filename.name,
            username='lcbruit',
            password='liI5HWjqZqJXdmrwfxyJ5hV779rx4B',
            )

        self.decrypt_file(
            input_filename=downloaded_filename.name,
            output_filename=decrypted_filename.name,
            password='NDknW80c87xqvlMtsHcSlzEvJ5vHMx',
        )

        self.unzip_file(
            input_filename=decrypted_filename.name,
            output_filename=unzipped_filename.name,
        )

        self.create_database(
            input_filename=unzipped_filename.name,
        )

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

    def create_database(self, input_filename):
        proc = subprocess.run([
            'cat',
            input_filename,
        ])

        if proc.returncode != 0:
            raise Exception('Could create database = {}.'.format(proc.returncode))
