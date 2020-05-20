#!/usr/bin/env python3

import itertools
from urllib import parse
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.model import CrfmStudy
from api.selenium import (
    SeleniumGrid,
    get_td_column_contents,
    get_td_keyvalue_contents,
    get_td_keyvalue_pairs,
)
from api.database import etl_central_session
from api.environment import CRFM_BASE_URL, CRFM_DB_ID
from api.uhl_etl.crf_manager import login


class CrfmStudyDetailDownload(SeleniumEtl):

    def __init__(self):
        super().__init__(schedule=Schedule.daily_7pm)

    def do_selenium_etl(self, driver):

        self._studies = []

        STUDY_LIST_URL = 'Print/Print_List.aspx?dbid={}&areaID=44&type=Query&name=Default&vid=&iid='.format(CRFM_DB_ID)

        login(driver)

        driver.get(parse.urljoin(
            CRFM_BASE_URL,
            STUDY_LIST_URL,
        ))

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR  , 'div.printarea > div:nth-child(3)'))
        )

        soup = BeautifulSoup(driver.page_source, "lxml")

        for p in soup.select('div.printarea > div:nth-child(3) > table'):
            full_title = p.find('thead').find('td').text
            title, study_number = itertools.islice(
                itertools.chain(
                    reversed(full_title.split(':', 1)),
                    itertools.repeat('', 2),
                ),
                2
            )

            self.log("Getting study details '{}'".format(title))

            iras_number = ethics_number = clinical_trial_gov = \
                eudract = isrctn = nihr_crn_number = protocol_number = \
                rd_number = who = status = ''

            for tr in p.tbody.find_all('tr'):

                if (tr.td.string or '').strip().startswith('IRAS Number'):
                    iras_number = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('Ethics Number'):
                    ethics_number = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('Clinical Trials Gov'):
                    clinical_trial_gov = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('Eudract'):
                    eudract = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('ISRCTN'):
                    isrctn = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('NIHR CRN Number'):
                    nihr_crn_number = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('Protocol Number'):
                    protocol_number = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('R & D Number'):
                    rd_number = (tr.td.next_sibling.string or '').strip()
                elif (tr.td.string or '').strip().startswith('WHO'):
                    who = (tr.td.next_sibling.string or '').strip()

            # This code relies on the previous loop leaving 'tr' as the
            # last row in the table
            status = (tr.select('td')[-1].string or '').strip()

            self._studies.append(CrfmStudy(
                study_number=study_number,
                title=title,
                protocol_number=protocol_number,
                ethics_number=ethics_number,
                clinical_trial_gov=clinical_trial_gov,
                isrctn=isrctn,
                iras_number=iras_number,
                nihr_crn_number=nihr_crn_number,
                rd_number=rd_number,
                who=who,
                eudract=eudract,
                status=status,
            ))

    def do_post_selenium_etl(self):

        with etl_central_session() as session:

            session.execute('DELETE FROM {};'.format(CrfmStudy.__tablename__))

            for s in self._studies:
                session.add(s)
