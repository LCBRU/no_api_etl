#!/usr/bin/env python3

import os
import urllib
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from api.core import Etl
from api.selenium import SeleniumGrid


class StudyDetailDownload(Etl):

    BASE_URL = 'https://www.edge.nhs.uk'
    PAGE_URL = 'DataQuality/index.php?pid={}'
    REDCAP_USERNAME = os.environ.get("REDCAP_USERNAME", '')
    REDCAP_PASSWORD = os.environ.get("REDCAP_PASSWORD", '')

    def get_report(self):

        with SeleniumGrid(SeleniumGrid.CHROME) as driver:

            self.login(driver)

            dq_page_url = urllib.parse.urljoin(
                self._redcap_instance()['base_url'],
                self.PAGE_URL.format(self._project_id),
            )

            driver.get(dq_page_url)

            driver.save_screenshot('screenshot.png')

            questionnaire_name = driver.find_element_by_id(
                "subheaderDiv2"
            ).text

    def login(self, driver):
        driver.get(StudyDetailDownload.BASE_URL)

        username = WebDriverWait(driver, 10).until(
            expected_conditions.presence_of_element_located((
                By.NAME, "username"))
        )

        password = driver.find_element_by_name('password')

        username.send_keys(self.REDCAP_USERNAME)
        password.send_keys(self.REDCAP_PASSWORD + Keys.RETURN)

    def get_count(self, driver, identifier):
            driver.find_element_by_xpath(
                "//div[@id='{}']/button".format(identifier)
            ).click()

            return driver.find_element_by_xpath(
                "//div[@id='{}']/div[1]".format(identifier)
            ).text
