#!/usr/bin/env python3

import urllib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import Etl, Schedule
from api.model import EdgeStudy
from api.selenium import SeleniumGrid, get_td_column_contents
from api.database import database
from api.environment import (
    EDGE_PASSWORD,
    EDGE_USERNAME,
    EDGE_BASE_URL,
)


class EdgeStudyDetailDownload(Etl):

    PAGE_URL = 'ProjectOverviewReport'

    def __init__(self):
        super().__init__(schedule=Schedule.daily)

    def do_etl(self):

        with database() as session:

            session.execute('DELETE FROM {};'.format(EdgeStudy.__tablename__))

            with SeleniumGrid(SeleniumGrid.CHROME) as driver:

                self.login(driver)

                driver.get(urllib.parse.urljoin(
                    EDGE_BASE_URL,
                    self.PAGE_URL,
                ))

                table_body = WebDriverWait(driver, 10).until(
                    ec.presence_of_element_located((By.ID, "divResults"))
                ).find_element_by_tag_name('table').find_element_by_tag_name('tbody')

                for row in table_body.find_elements_by_xpath('tr'):                
                    site = get_td_column_contents(row, 2)
                    portfolio_number = get_td_column_contents(row, 4)
                    title = get_td_column_contents(row, 6)
                    status = get_td_column_contents(row, 7)
                    type = get_td_column_contents(row, 8)

                    s = EdgeStudy(
                        title=title,
                        site=site,
                        portfolio_number=portfolio_number,
                        status=status,
                        type=type,
                    )
                    session.add(s)

    def login(self, driver):
        driver.get(EDGE_BASE_URL)

        username = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((
                By.NAME, "fldUsername"))
        )

        password = driver.find_element_by_name('fldPassword')

        username.send_keys(EDGE_USERNAME)
        password.send_keys(EDGE_PASSWORD)

        password.send_keys(Keys.RETURN)

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.ID, "headerOrganisationName"))
        )
