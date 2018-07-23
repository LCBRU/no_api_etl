#!/usr/bin/env python3

import os
import urllib
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from api.core import Etl, Schedule
from api.selenium import SeleniumGrid


class StudyDetailDownload(Etl):

    BASE_URL = 'https://www.edge.nhs.uk'
    PAGE_URL = 'ProjectOverviewReport'
    REDCAP_USERNAME = os.environ.get("EDGE_USERNAME", '')
    REDCAP_PASSWORD = os.environ.get("EDGE_PASSWORD", '')

    def __init__(self):
        super().__init__(schedule=Schedule.daily)

    def do_etl(self):

        with SeleniumGrid(SeleniumGrid.CHROME) as driver:

            self.login(driver)

            dq_page_url = urllib.parse.urljoin(
                self.BASE_URL,
                self.PAGE_URL,
            )

            results = WebDriverWait(driver, 10).until(
                        expected_conditions.text_to_be_present_in_element_value((
                            By.ID, "headerOrganisationName"), 'Leicester')
                    )

            print(dq_page_url)

            driver.get(dq_page_url)

            results = WebDriverWait(driver, 10).until(
                        expected_conditions.presence_of_element_located((
                            By.ID, "divResults"))
                    )
            
            driver.save_screenshot('success.png')

    def login(self, driver):
        driver.get(StudyDetailDownload.BASE_URL)

        username = WebDriverWait(driver, 10).until(
            expected_conditions.presence_of_element_located((
                By.NAME, "fldUsername"))
        )

        password = driver.find_element_by_name('fldPassword')

        username.send_keys(self.REDCAP_USERNAME)
        password.send_keys(self.REDCAP_PASSWORD)

        driver.save_screenshot('login.png')

        password.send_keys(Keys.RETURN)

        driver.save_screenshot('login2.png')
