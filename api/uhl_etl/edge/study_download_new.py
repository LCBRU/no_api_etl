#!/usr/bin/env python3

import time
import datetime
import re
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


class EdgeStudyDetailDownloadNew(SeleniumEtl):

    PAGE_URL = 'ProjectSearch2/GetProjSearchPrintableSummary'

    def __init__(self):
        super().__init__(schedule=Schedule.never)

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

        driver.get(parse.urljoin(
            EDGE_BASE_URL,
            self.PAGE_URL,
        ))

        table_body = WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//table/tbody'))
        )

        for row in table_body.find_elements_by_xpath('tr'):
            links = row.find_elements(By.TAG_NAME, 'a')

            for l in links:
                result.append(l.get_attribute('href'))

        return set(result)

    def get_study_links(self, driver, study_site_links):
        result = []

        for l in study_site_links:
            driver.get(l)

            sl = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.CSS_SELECTOR  , 'h2.projectSite a'))
            ).get_attribute('href')

            result.append(sl)

        return set(result)

    def get_study_details(self, driver, study_links):
        result = []

        for l in study_links:
            driver.get(l)

            identifiers = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="identifiers"]/table/tbody'))
            )
            details = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table/tbody'))
            )
            participants = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="participants"]/table/tbody'))
            )

            result.append(EdgeStudy(
                edge_study_id=l.split('/')[-1],
                title=get_td_keyvalue_contents(details, 'Short Title'),
                status=get_td_keyvalue_contents(details, 'Status'),
                type=get_td_keyvalue_contents(details, 'Project Type'),
                chief_investigator=get_td_keyvalue_contents(details, 'Chief Investigator'),
                planned_start_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(details, 'Planned Start Date')
                ),
                start_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(details, 'Start Date')
                ),
                planned_end_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(details, 'Planned End Date')
                ),
                end_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(details, 'End Date')
                ),

                local_project_reference=get_td_keyvalue_contents(identifiers, 'Local Project Reference'),
                nihr_portfolio_study_id=get_td_keyvalue_contents(identifiers, 'NIHR Portfolio Study ID'),
                iras_number=get_td_keyvalue_contents(identifiers, 'IRAS Number'),
                mrec_number=get_td_keyvalue_contents(identifiers, 'MREC Number'),

                target_size=get_td_keyvalue_contents(participants, 'Target size'),
                actual_recruitment=get_td_keyvalue_contents(participants, 'Actual recruitment').split(" ")[0].strip(),
            ))

        return result


    def get_study_site_details(self, driver, study_site_links):
        result = []

        whitespace = re.compile('\s')

        for l in study_site_links:
            driver.get(l)

            study_link = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.CSS_SELECTOR  , 'h2.projectSite a'))
            ).get_attribute('href')
            
            identifiers = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="identifiers"]/div/div/table[position()=1]/tbody'))
            )
            details = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table[position()=1]/tbody'))
            )
            approvals = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table[position()=2]/tbody'))
            )
            milestones = WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH  , '//div[@id="details"]/table[position()=3]/tbody'))
            )

            result.append(EdgeStudySite(
                edge_study_site_id=l.split('/')[-1],
                edge_study_id=study_link.split('/')[-1],
                site=whitespace.sub(' ', get_td_keyvalue_contents(details, 'Site')).split("(")[0].strip(),
                status=get_td_keyvalue_contents(details, 'Status'),
                site_type=get_td_keyvalue_contents(details, 'Type'),
                principal_investigator=get_td_keyvalue_contents(details, 'Principal Investigator'),
                site_target_recruitment=get_td_keyvalue_contents(details, 'Site target recruitment'),

                approval_process=get_td_keyvalue_contents(approvals, 'Approval process'),
                randd_submission_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'R&D Submission Date')
                ),
                start_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Start date')
                ),
                ssi_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'SSI date')
                ),
                candc_assessment_required=get_td_keyvalue_contents(approvals, 'Capacity & capability'),
                date_site_invited=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Date site invited')
                ),
                date_site_selected=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Date site selected')
                ),
                date_site_confirmed_by_sponsor=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Date site confirmed by Sponsor')
                ),
                date_site_confirmed=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Date site confirmed:')
                ),
                non_confirmation_status=get_td_keyvalue_contents(approvals, 'Non confirmation status'),
                date_of_non_confirmation=self.parsed_date_or_none(
                    get_td_keyvalue_contents(approvals, 'Date of non confirmation')
                ),

                siv_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'SIV date')
                ),
                open_to_recruitment_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'Open to recruitment')
                ),
                recruitment_start_date_date_planned=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'Recruitment end date (Planned)')
                ),
                recruitment_start_date_date_actual=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'Recruitment end date (Actual)')
                ),
                planned_closing_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'Planned closing date')
                ),
                closed_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(milestones, 'Closed date')
                ),

                first_patient_consented=self.parsed_date_or_none(
                    get_td_keyvalue_contents(identifiers, 'First patient consented')
                ),
                first_patient_recruited=self.parsed_date_or_none(
                    get_td_keyvalue_contents(identifiers, 'First patient recruited')
                ),
                first_patient_recruited_consent_date=self.parsed_date_or_none(
                    get_td_keyvalue_contents(identifiers, 'First patient recruited (Consent date)')
                ),
                recruitment_clock_days=get_td_keyvalue_contents(identifiers, 'Recruitment clock'),
                ssi_to_first_patient_days=get_td_keyvalue_contents(identifiers, 'SSI to first patient'),
                estimated_annual_target=get_td_keyvalue_contents(identifiers, 'Estimated Annual Target'),
                estimated_months_running=get_td_keyvalue_contents(identifiers, 'Estimated Months Running'),
                actual_recruitment=whitespace.sub(' ', get_td_keyvalue_contents(identifiers, 'Actual recruitment')).split(' ')[0].strip(),
                last_patient_recruited=self.parsed_date_or_none(
                    get_td_keyvalue_contents(identifiers, 'Last patient recruited')
                ),
                last_patient_referred=self.parsed_date_or_none(
                    get_td_keyvalue_contents(identifiers, 'Last patient referred')
                ),
            ))
        
        return result

    def parsed_date_or_none(self, date_string):
        if date_string:
            return datetime.datetime.strptime(
                date_string,
                "%d/%m/%Y"
            ).date()
        else:
            return None
