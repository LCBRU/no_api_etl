#!/usr/bin/env python3

import time
import datetime
import re
from bs4 import BeautifulSoup
from urllib import parse
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.model import EdgeStudy, EdgeStudySite
from api.selenium import SeleniumGrid, get_td_keyvalue_contents
from api.database import etl_central_session
from api.environment import EDGE_BASE_URL
from api.uhl_etl.edge import login


class EdgeStudyDetailDownload(SeleniumEtl):

    PRINT_URL = 'ProjectSearch2/GetProjSearchPrintableSummary'
    SEARCH_URL = 'ProjectSearch2'

    def __init__(self):
        super().__init__(schedule=Schedule.daily)

    def do_selenium_etl(self, driver):

        with etl_central_session() as session:

            login(driver)

            study_site_links = self.get_study_site_links(driver)
            study_site_details = self.get_study_site_details(driver, study_site_links)
            study_links = self.get_study_links(driver, study_site_links)
            study_details = self.get_study_details(driver, study_links)

            session.execute('DELETE FROM {};'.format(EdgeStudy.__tablename__))
            session.execute('DELETE FROM {};'.format(EdgeStudySite.__tablename__))

            for s in study_details:
                session.add(s)
            for s in study_site_details:
                session.add(s)


    def get_study_site_links(self, driver):
        result = []

        # For some reason (known only to Southampton University and the Devil),
        # The print friendly report takes its parameters from the last
        # time the non-print friendly version was run - with all the craziness
        # that can then ensue.
        
        driver.get(parse.urljoin(
            EDGE_BASE_URL,
            self.SEARCH_URL,
        ))

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//div[@id="projects"]'))
        )

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//a[@id="lnkProjects_Organsation"]'))
        ).click()

        time.sleep(2)

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//a[@id="lnkProjects_Organsation"]'))
        ).click()

        time.sleep(2)

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//div[@id="projects"]'))
        )

        driver.get(parse.urljoin(
            EDGE_BASE_URL,
            self.PRINT_URL,
        ))

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//table/tbody'))
        )

        soup = BeautifulSoup(driver.page_source, "lxml")

        for link in soup.find_all('a'):
            result.append(link['href'])

        return set(result)

    def get_study_links(self, driver, study_site_links):
        result = []

        for l in study_site_links:
            driver.get(parse.urljoin(
                EDGE_BASE_URL,
                l,
            ))

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table[position()=1]/tbody'))
            )

            soup = BeautifulSoup(driver.page_source, "lxml")

            study_link = soup.find("h2", class_="projectSite").a

            result.append(study_link['href'])

        return set(result)

    def get_study_details(self, driver, study_links):
        result = []

        for l in study_links:
            driver.get(parse.urljoin(
                EDGE_BASE_URL,
                l,
            ))

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]'))
            )

            soup = BeautifulSoup(driver.page_source, "lxml")

            identifier_values = self.get_td_keyvalue_pairs(soup.find(id="identifiers").table)
            detail_values = self.get_td_keyvalue_pairs(soup.find(id="details").table)
            participant_values = self.get_td_keyvalue_pairs(soup.find(id="participants").table)

            e= EdgeStudy(

                # Details

                edge_study_id=l.split('/')[-1],
                title=detail_values.get('Short Title:'),
                full_title=detail_values.get('Full title:'),
                status=detail_values.get('Status:'),
                type=detail_values.get('Project Type:'),
                chief_investigator=detail_values.get('Chief Investigator:'),
                planned_start_date=self.parsed_date_or_none(
                    detail_values.get('Planned Start Date:')
                ),
                start_date=self.parsed_date_or_none(
                    detail_values.get('Start Date:')
                ),
                planned_end_date=self.parsed_date_or_none(
                    detail_values.get('Planned End Date:')
                ),
                end_date=self.parsed_date_or_none(
                    detail_values.get('End Date:')
                ),

                # Identifiers

                local_project_reference=identifier_values.get('Local Project Reference:'),
                nihr_portfolio_study_id=identifier_values.get('NIHR Portfolio Study ID:'),
                iras_number=identifier_values.get('IRAS Number:'),
                mrec_number=identifier_values.get('MREC Number:'),

                # Participants

                target_size=participant_values.get('Target size:'),
                actual_recruitment=(participant_values.get('Actual recruitment:') or '').split(" ")[0].strip(),
            )

            result.append(e)

        return result


    def get_study_site_details(self, driver, study_site_links):
        result = []

        whitespace = re.compile('\s')

        for l in study_site_links:

            driver.get(parse.urljoin(
                EDGE_BASE_URL,
                l,
            ))

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table[position()=1]/tbody'))
            )

            soup = BeautifulSoup(driver.page_source, "lxml")

            details_tables = list(soup.find(id="details").find_all('table'))

            identifier_values = self.get_td_keyvalue_pairs(soup.find(id="identifiers").table)
            detail_values = self.get_td_keyvalue_pairs(details_tables[0])
            approval_values = self.get_td_keyvalue_pairs(details_tables[1])
            milestones_values = self.get_td_keyvalue_pairs(details_tables[2])

            study_link = soup.find("h2", class_="projectSite").a

            e = EdgeStudySite(

                # Study Details

                edge_study_site_id=l.split('/')[-1],
                edge_study_id=study_link['href'].split('/')[-1],
                site=whitespace.sub(' ', detail_values.get('Site (Parent):')).split("(")[0].strip(),
                status=detail_values.get('Status:'),
                site_type=detail_values.get('Type:'),
                principal_investigator=detail_values.get('Principal Investigator:'),
                site_target_recruitment=detail_values.get('Site target recruitment:'),

                # Approvals

                approval_process=approval_values.get('Approval process:'),
                randd_submission_date=self.parsed_date_or_none(
                    approval_values.get('R&D Submission Date:')
                ),
                start_date=self.parsed_date_or_none(
                    approval_values.get('Start date (NHS Permission):')
                ),
                ssi_date=self.parsed_date_or_none(
                    approval_values.get('SSI date:')
                ),
                candc_assessment_required=approval_values.get('Capacity & capability assessment required?:'),
                date_site_invited=self.parsed_date_or_none(
                    approval_values.get('Date site invited:')
                ),
                date_site_selected=self.parsed_date_or_none(
                    approval_values.get('Date site selected:')
                ),
                date_site_confirmed_by_sponsor=self.parsed_date_or_none(
                    approval_values.get('Date site confirmed by Sponsor:')
                ),
                date_site_confirmed=self.parsed_date_or_none(
                    approval_values.get('Date site confirmed:')
                ),
                non_confirmation_status=approval_values.get('Non confirmation status:'),
                date_of_non_confirmation=self.parsed_date_or_none(
                    approval_values.get('Date of non confirmation:')
                ),

                # Milestones

                siv_date=self.parsed_date_or_none(
                    milestones_values.get('SIV date:')
                ),
                open_to_recruitment_date=self.parsed_date_or_none(
                    milestones_values.get('Open to recruitment:')
                ),
                recruitment_start_date_date_planned=self.parsed_date_or_none(
                    milestones_values.get('Recruitment end date (Planned):')
                ),
                recruitment_start_date_date_actual=self.parsed_date_or_none(
                    milestones_values.get('Recruitment end date (Actual):')
                ),
                planned_closing_date=self.parsed_date_or_none(
                    milestones_values.get('Planned closing date:')
                ),
                closed_date=self.parsed_date_or_none(
                    milestones_values.get('Closed date:')
                ),

                # Identifiers

                first_patient_consented=self.parsed_date_or_none(
                    identifier_values.get('First patient consented:')
                ),
                first_patient_recruited=self.parsed_date_or_none(
                    identifier_values.get('First patient recruited:')
                ),
                first_patient_recruited_consent_date=self.parsed_date_or_none(
                    identifier_values.get('First patient recruited (Consent date):')
                ),
                recruitment_clock_days=identifier_values.get('Recruitment clock (days):'),
                ssi_to_first_patient_days=identifier_values.get('SSI to first patient (days):'),
                estimated_annual_target=identifier_values.get('Estimated Annual Target:'),
                estimated_months_running=identifier_values.get('Estimated Months Running:'),
                actual_recruitment=whitespace.sub(' ', identifier_values.get('Actual recruitment:')).split(' ')[0].strip(),
                last_patient_recruited=self.parsed_date_or_none(
                    identifier_values.get('Last patient recruited:')
                ),
                last_patient_referred=self.parsed_date_or_none(
                    identifier_values.get('Last patient referred:')
                ),
            )

            result.append(e)
        
        return result

    def parsed_date_or_none(self, date_string):
        if date_string:
            return datetime.datetime.strptime(
                date_string,
                "%d/%m/%Y"
            ).date()
        else:
            return None


    def get_td_keyvalue_pairs(self, table):
        result = {}

        for tr in table.tbody.find_all('tr'):
            td = tr.find_all('td')

            key = td[0].get_text().strip()
            value = td[1].get_text().strip()

            result[key] = value

        return result
