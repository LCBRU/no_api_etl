#!/usr/bin/env python3

import itertools
from urllib import parse
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.model import CrfmStudy
from api.selenium import SeleniumGrid, get_td_column_contents, get_td_keyvalue_contents
from api.database import database
from api.environment import CRFM_BASE_URL, CRFM_DB_ID
from api.uhl_etl.crf_manager import login


class CrfmStudyDetailDownload(SeleniumEtl):

    def __init__(self):
        super().__init__(schedule=Schedule.five_minutely)

    def do_selenium_etl(self, driver):

        STUDY_LIST_URL = 'Print/Print_List.aspx?dbid={}&areaID=44&type=Query&name=Default&vid=&iid='.format(CRFM_DB_ID)

        with database() as session:

            session.execute('DELETE FROM {};'.format(CrfmStudy.__tablename__))

            login(driver)

            driver.get(parse.urljoin(
                CRFM_BASE_URL,
                STUDY_LIST_URL,
            ))

            projects = driver.find_element(By.CSS_SELECTOR, 'div.printarea > div:nth-child(3)')

            for p in projects.find_elements(By.TAG_NAME, 'table')[1:]:
                full_title = p.find_element(By.CSS_SELECTOR, 'thead td').text
                title, portfolio_number = itertools.islice(
                    itertools.chain(
                        reversed(full_title.split(':', 1)),
                        itertools.repeat('', 2),
                    ),
                    2
                )
                
                table_body = p.find_element(By.XPATH, 'tbody')
                rd_number = get_td_keyvalue_contents(table_body, 'R & D Number')
                crn_number = get_td_keyvalue_contents(table_body, 'NIHR CRN Number')
                
                study_details = table_body.find_elements(By.XPATH, 'tr[contains(td/text(), "Study Details")]/following-sibling::tr')[1]
                status = get_td_column_contents(study_details, 7).replace('<br>', '')

                s = CrfmStudy(
                    portfolio_number=portfolio_number,
                    title=title,
                    rd_number=rd_number,
                    crn_number=crn_number,
                    status=status,
                )

                session.add(s)
