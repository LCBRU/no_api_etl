#!/usr/bin/env python3

import datetime
from bs4 import BeautifulSoup
from urllib import parse
from api.core import SeleniumEtl, Schedule
from api.database import etl_central_session
from api.environment import EDGE_BASE_URL
from api.uhl_etl.edge import login
from lbrc_edge import EdgeSiteStudy


class EdgeSiteStudyDownload(SeleniumEtl):

    REPORT_FOLDER = 'SharedReports'
    REPORT = 'BRC Report (Tara)'

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
                project_id=self.int_or_none(td[0]),
                mrec_number=self.string_or_none(td[1]),
                iras_number=self.string_or_none(td[2]),
                project_full_title=self.string_or_none(td[3]),
                project_short_title=self.string_or_none(td[4]),
                project_phase=self.string_or_none(td[5]),
                primary_clinical_management_areas=self.string_or_none(td[6]),
                project_site_status=self.string_or_none(td[7]),
                project_status=self.string_or_none(td[8]),
                project_site_rand_submission_date=self.date_or_none(td[9]),
                project_site_start_date_nhs_permission=self.date_or_none(td[10]),
                project_site_date_site_confirmed=self.date_or_none(td[11]),
                project_site_planned_closing_date=self.date_or_none(td[12]),
                project_site_closed_date=self.date_or_none(td[13]),
                project_site_planned_recruitment_end_date=self.date_or_none(td[14]),
                project_site_actual_recruitment_end_date=self.date_or_none(td[15]),
                principle_investigator=self.string_or_none(td[16]),
                project_site_target_participants=self.int_or_none(td[17]),
                project_site_estimated_annual_target=self.int_or_none(td[18]),
                recruited_org=self.int_or_none(td[19]),
                project_site_lead_nurses=self.string_or_none(td[20]),
                project_site_name=self.string_or_none(td[21]),
                project_type=self.string_or_none(td[22]),
                nihr_portfolio_study_id=self.int_or_none(td[23]),
                pi_orcidid=self.string_or_none(td[24]),
                is_uhl_lead_centre=self.boolean_or_none(td[25]),
                lead_centre_name_if_not_uhl=self.boolean_or_none(td[26]),
                cro_cra_used=self.boolean_or_none(td[27]),
                name_of_cro_cra_company_used=self.string_or_none(td[28]),
                study_category=self.string_or_none(td[29]),
                randomised_name=self.string_or_none(td[30]),
                name_of_brc_involved=self.string_or_none(td[31]),
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

    def boolean_or_none(self, boolean_element):
        boolean_string = boolean_element.get_text().strip().upper()
        if boolean_string in ['YES', 'TRUE', '1']:
            return True
        elif boolean_string in ['NO', 'FALSE', '0']:
            return False
        else:
            return None
