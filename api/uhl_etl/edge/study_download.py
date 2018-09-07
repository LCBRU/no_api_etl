#!/usr/bin/env python3

import time
import datetime
from urllib import parse
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.model import EdgeStudy
from api.selenium import SeleniumGrid, get_td_column_contents
from api.database import database
from api.environment import EDGE_BASE_URL
from api.uhl_etl.edge import login


class EdgeStudyDetailDownload(SeleniumEtl):

    PAGE_URL = 'ProjectOverviewReport'

    def __init__(self):
        super().__init__(schedule=Schedule.hourly)

    def do_selenium_etl(self, driver):

        with database() as session:

            session.execute('DELETE FROM {};'.format(EdgeStudy.__tablename__))

            login(driver)

            driver.get(parse.urljoin(
                EDGE_BASE_URL,
                self.PAGE_URL,
            ))

            table_body = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//div[@id = "divResults" and contains(p/text(), "Records returned")]'))
            )

            fldLimitToOrganisation = driver.find_element(By.ID, 'fldLimitToOrganisation')

            if fldLimitToOrganisation.get_attribute('checked'):
                fldLimitToOrganisation.click()

            submitQuery = driver.find_element(By.XPATH, '//input[@value = "Submit query"]')
            submitQuery.click()

            time.sleep(5) # Wait for submission to have started

            table_body = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//div[@id = "divResults" and contains(p/text(), "Records returned")]'))
            ).find_element_by_tag_name('table').find_element_by_tag_name('tbody')

            for row in table_body.find_elements_by_xpath('tr'):
                s = EdgeStudy(
                    site=get_td_column_contents(row, 2),
                    portfolio_number= get_td_column_contents(row, 4),
                    title=get_td_column_contents(row, 6),
                    status=get_td_column_contents(row, 7),
                    type=get_td_column_contents(row, 8),
                    designs=get_td_column_contents(row, 9),
                    site_type=get_td_column_contents(row, 11),
                    recruitment_start_date=self.parsed_date_or_none(
                        get_td_column_contents(row, 12)),
                    recruitment_end_date=self.parsed_date_or_none(
                        get_td_column_contents(row, 13)),
                    recruitment_target=get_td_column_contents(row, 14),
                    recruitment_so_far=get_td_column_contents(row, 15),
                )

                session.add(s)

    def parsed_date_or_none(self, date_string):
        if date_string:
            return datetime.datetime.strptime(
                date_string,
                "%d/%m/%Y"
            ).date()
        else:
            return None