#!/usr/bin/env python3

import datetime
from bs4 import BeautifulSoup
from urllib import parse
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.database import etl_central_session
from api.environment import EDGE_BASE_URL
from api.uhl_etl.edge import login
from sqlalchemy import Column, Integer, String, Date
from api.database import Base


class EdgeSiteStudy(Base):
    __tablename__ = 'edge_site_study'

    id = Column(Integer, primary_key=True)
    project_short_title = Column(String)
    project_id = Column(Integer)
    principle_investigator = Column(String)
    project_type = Column(String)
    research_theme = Column(String)
    start_date = Column(Date)
    project_site_date_open_to_recruitment = Column(Date)
    project_site_start_date_nhs_permission = Column(Date)
    end_date = Column(Date)
    project_site_closed_date = Column(Date)
    project_site_planned_closing_date = Column(Date)
    recruitment_end_date = Column(Date)
    project_site_actual_recruitment_end_date = Column(Date)
    project_site_planned_recruitment_end_date = Column(Date)
    recruited_total = Column(Integer)
    recruited_org = Column(Integer)
    project_status = Column(String)
    project_site_target_participants = Column(Integer)


class EdgeSiteStudyDownload(SeleniumEtl):

    REPORT_FOLDER = 'SharedReports'
    REPORT = 'No API Study Details Download'

    def __init__(self):
        super().__init__(schedule=Schedule.daily_7pm)

    def do_selenium_etl(self, driver):
        self._studies = self.get_studies(driver)

    def do_post_selenium_etl(self):
        with etl_central_session() as session:
            self.log("Deleting old studies")

            session.query(EdgeSiteStudy).delete()

            self.log("Creating new studies")
            session.add_all(self._studies)

    def get_studies(self, driver):
        self.log("Getting studies")

        result = []

        login(driver)

        driver.get(parse.urljoin(
            EDGE_BASE_URL,
            self.REPORT_FOLDER,
        ))

        driver.find_element_by_xpath(f'//select/option[text()="{self.REPORT}"]').click()
        driver.find_element_by_xpath('//input[@type="button" and @value="Submit query"]').click()
        driver.find_element_by_xpath('//div[@id="results"]/table')

        soup = BeautifulSoup(driver.page_source, "lxml")

        results = soup.find(id='results').find('table').find('tbody')

        for tr in results.find_all('tr'):
            td = tr.find_all('td')

            self.log("Downloading study '{}'".format(self.string_or_none(td[0])))

            e = EdgeSiteStudy(
                project_short_title=self.string_or_none(td[0]),
                project_id=self.int_or_none(td[1]),
                principle_investigator=self.string_or_none(td[2]),
                project_type=self.string_or_none(td[3]),
                research_theme=self.string_or_none(td[4]),
                start_date=self.date_or_none(td[5]),
                project_site_date_open_to_recruitment=self.date_or_none(td[6]),
                project_site_start_date_nhs_permission=self.date_or_none(td[7]),
                end_date=self.date_or_none(td[8]),
                project_site_closed_date=self.date_or_none(td[9]),
                project_site_planned_closing_date=self.date_or_none(td[10]),
                recruitment_end_date=self.date_or_none(td[11]),
                project_site_actual_recruitment_end_date=self.date_or_none(td[12]),
                project_site_planned_recruitment_end_date=self.date_or_none(td[13]),
                recruited_total=self.int_or_none(td[14]),
                recruited_org=self.string_or_none(td[15]),
                project_status=self.string_or_none(td[16]),
                project_site_target_participants=self.int_or_none(td[17]),
            )

            result.append(e)

        self.log("Getting studies: COMPLETED")

        return result

    def string_or_none(self, string_element):
        string_element = string_element.get_text().strip()
        if string_element:
            return string_element
        else:
            return None

    def int_or_none(self, int_element):
        int_string = int_element.get_text().strip()
        if int_string:
            return int(int_string)
        else:
            return None

    def date_or_none(self, date_element):
        date_string = date_element.get_text().strip()
        if date_string:
            return datetime.datetime.strptime(date_string, "%d/%m/%Y").date()
        else:
            return None
