#!/usr/bin/env python3

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
