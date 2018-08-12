#!/usr/bin/env python3

import urllib
import itertools
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import Etl, Schedule
from api.model import CrfmStudy
from api.selenium import SeleniumGrid, get_td_column_contents, get_td_keyvalue_contents
from api.database import database
from api.environment import (
    CRFM_PASSWORD,
    CRFM_USERNAME,
    CRFM_BASE_URL,
)


class CrfmStudyDetailDownload(Etl):

    def __init__(self):
        super().__init__(schedule=Schedule.daily)

    def do_etl(self):

        STUDY_LIST_URL = 'Print/Print_List.aspx?dbid=crf_leicestercrf_test&areaID=44&type=Query&name=Default&vid=&iid='

        with database() as session:

            session.execute('DELETE FROM {};'.format(CrfmStudy.__tablename__))

            with SeleniumGrid(SeleniumGrid.CHROME) as driver:

                self.login(driver)

                driver.get(urllib.parse.urljoin(
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

    def login(self, driver):
        driver.get(urllib.parse.urljoin(
                CRFM_BASE_URL,
                'Login.aspx',
            ))

        username = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((
                By.ID, "tbLogin"))
        )

        password = driver.find_element(By.ID, 'tbPassword')

        username.send_keys(CRFM_USERNAME)
        password.send_keys(CRFM_PASSWORD)

        password.send_keys(Keys.RETURN)

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, "div.pnl_primary_links"))
        )
