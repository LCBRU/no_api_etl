#!/usr/bin/env python3

import time
import datetime
import re
import pprint
from bs4 import BeautifulSoup
from urllib import parse
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.core import SeleniumEtl, Schedule
from api.model import EdgeStudy, EdgeStudySite, EdgeAnnualReport
from api.selenium import SeleniumGrid
from api.database import etl_central_session
from api.environment import EDGE_BASE_URL
from api.uhl_etl.edge import login
from api.emailing import email_error


class EdgeStudyDetailDownload(SeleniumEtl):

    PRINT_URL = 'ProjectSearch2/GetProjSearchPrintableSummary'
    SEARCH_URL = 'ProjectSearch2'

    def __init__(self):
        super().__init__(schedule=Schedule.daily_7pm)

    def do_selenium_etl(self, driver):

            login(driver)

            study_site_links = self.get_study_site_links(driver)
            self._study_site_details = self.get_study_site_details(driver, study_site_links)
            study_links = self.get_study_links(driver, study_site_links)
            self._study_details = self.get_study_details(driver, study_links)

    def do_post_selenium_etl(self):
        with etl_central_session() as session:

            session.execute('DELETE FROM {};'.format(EdgeStudy.__tablename__))
            session.execute('DELETE FROM {};'.format(EdgeStudySite.__tablename__))

            for s in self._study_details:
                session.add(s)
            for s in self._study_site_details:
                session.add(s)


    def get_study_site_links(self, driver):
        result = []

        # For some reason (known only to Southampton University and the Devil),
        # The print friendly report takes its parameters from the last
        # time the non-print friendly version was run - with all the craziness
        # that then ensues.
        
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
            self.log("Getting study details '{}'".format(l))

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
            attribute_values = self.get_attribute_keyvalue_pairs(driver)

            e = EdgeStudy(

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
                disease_area=detail_values.get('Disease area:').split(" ")[0].strip(),

                # Identifiers

                local_project_reference=identifier_values.get('Local Project Reference:'),
                nihr_portfolio_study_id=identifier_values.get('NIHR Portfolio Study ID:'),
                iras_number=identifier_values.get('IRAS Number:'),
                mrec_number=identifier_values.get('MREC Number:'),

                # Participants

                target_size=participant_values.get('Target size:'),
                actual_recruitment=(participant_values.get('Actual recruitment:') or '').split(" ")[0].strip(),

                # Attributes

                is_uhl_lead_centre=(attribute_values.get('Is UHL Lead Centre?') or '').lower() == 'yes',
                primary_clinical_management_areas=attribute_values.get('Primary Clinical Management Areas'),
            )

            result.append(e)

        return result


    def get_study_site_details(self, driver, study_site_links):

        result = []

        whitespace = re.compile('\s')

        for l in study_site_links:
            self.log("Getting study site details '{}'".format(l))

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

        for tr in table.find_all('tr'):
            td = tr.find_all('td')

            if len(td) > 1:
                key = td[0].get_text().strip()
                value = td[1].get_text().strip()

                result[key] = value

        return result


    def get_attribute_keyvalue_pairs(self, driver):
        result = {}

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//a[@id="tabAttributes"]'))
        ).click()

        time.sleep(2)

        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//a[text()="Mandatory Category 1 (UHL)"]'))
        ).click()

        soup = BeautifulSoup(driver.page_source, "lxml")

        for table in soup.find_all("table", attrs={"data-bind": "foreach: ProjectAttributes"}):
            for tr in table.find_all('tr'):
                td = tr.find_all('td')

                if len(td) > 3:
                    key = td[2].get_text().strip()
                    value = td[3].get_text().strip()

                    result[key] = value

        return result


class EdgeAnnualReportDownload(SeleniumEtl):

    REPORTS = 'SharedReports'

    def __init__(self):
        super().__init__(schedule=Schedule.daily_7pm)

    def do_selenium_etl(self, driver):
        self._studies = self.get_studies(driver)

    def do_post_selenium_etl(self):
        with etl_central_session() as session:
            self.log("Deleting old studies")

            session.execute('DELETE FROM {};'.format(EdgeAnnualReport.__tablename__))

            self.log("Creating new studies")
            for s in self._studies:
                session.add(s)

    def get_studies(self, driver):
        self.log("Getting studies")

        result = []

        with etl_central_session() as session:

            login(driver)

            driver.get(parse.urljoin(
                EDGE_BASE_URL,
                self.REPORTS,
            ))

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//select/option[text()="Dan BRC Annual Report"]'))
            ).click()
            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//input[@type="button" and @value="Submit query"]'))
            ).click()
            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//div[@id="results"]/table'))
            )

            soup = BeautifulSoup(driver.page_source, "lxml")

            results = soup.find(id='results').find('table').find('tbody')

            for tr in results.find_all('tr'):
                td = tr.find_all('td')

                self.log("Downloadiung study '{}'".format(td[2].get_text().strip()))

                e = EdgeAnnualReport(
                    project_id=int(td[0].get_text().strip()),
                    full_title=td[1].get_text().strip(),
                    short_title=td[2].get_text().strip(),
                    mrec_number=td[3].get_text().strip(),
                    principle_investigator=td[4].get_text().strip(),
                    pi_orcid=td[5].get_text().strip(),
                    start_date=self.parsed_date_or_none(td[6].get_text().strip()),
                    end_date=self.parsed_date_or_none(td[7].get_text().strip()),
                    status=td[8].get_text().strip(),
                    research_theme=td[9].get_text().strip(),
                    ukcrc_health_category=td[10].get_text().strip(),
                    main_speciality=td[11].get_text().strip(),
                    disease_area=td[12].get_text().strip(),
                    project_type=td[13].get_text().strip(),
                    primary_intervention_or_area=td[14].get_text().strip(),
                    randomisation=td[15].get_text().strip(),
                    recruited_total=int(td[16].get_text().strip()),
                    funders=td[17].get_text().strip(),
                    funding_category=td[18].get_text().strip(),
                    total_external_funding_awarded=td[19].get_text().strip(),
                    is_uhl_lead_centre=self.parsed_boolean_or_none(td[20].get_text().strip()),
                    lead_centrename_if_not_uhl=td[21].get_text().strip(),
                    multicentre=self.parsed_boolean_or_none(td[22].get_text().strip()),
                    first_in_human_centre=self.parsed_boolean_or_none(td[23].get_text().strip()),
                    link_to_nhir_translational_research_collaboration=self.parsed_boolean_or_none(td[24].get_text().strip()),
                )

                result.append(e)

            self.log("Getting studies: COMPLETED")

            return result


    def parsed_date_or_none(self, date_string):
        if date_string:
            return datetime.datetime.strptime(
                date_string,
                "%d/%m/%Y"
            ).date()
        else:
            return None

    def parsed_boolean_or_none(self, boolean_string):
        if boolean_string == 'Yes':
            return True
        elif boolean_string == 'No':
            return False
        else:
            return None



