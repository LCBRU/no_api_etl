import re
import itertools
from datetime import datetime, date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from pprint import pprint as pp
from sqlalchemy.inspection import inspect
from api.core import Schedule, Etl
from api.uhl_etl.hic_covid.model import (
	hic_covid_session,
    Administration,
    BloodTest,
    CriticalCarePeriod,
	Demographics,
    Diagnosis,
    Emergency,
    Episode,
    MicrobiologyTest,
    Observation,
    Order,
    Prescribing,
    Procedure,
    Transfer,
    Virology,
)
from api.database import uhl_dwh_databases_engine


NAMES_SQL = """
SELECT DISTINCT LOWER(name) AS name
FROM (
	SELECT SURNAME AS name
	FROM DWREPO.dbo.PATIENT
	UNION
	SELECT LEFT(forenames, PATINDEX('%[ -/]%', forenames + ' ') - 1) AS name
	FROM DWREPO.dbo.PATIENT
) x
WHERE name IS NOT NULL
	AND LEN(name) > 2
;
"""


class CovidPpiValidationEtl(Etl):
    re_numbers = re.compile(r'\d')
    re_words = re.compile(r'\W+')
    re_uhl_s_number = re.compile(r'([SRFG]\d{7}|[U]\d{7}.*|LB\d{7}|RTD[\-0-9]*)')
    re_postcodes = re.compile(r'([Gg][Ii][Rr] ?0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})')
    re_nhs_dividers = re.compile(r'[- ]')
    re_nhs_numbers = re.compile(r'(?=(\d{10}))')
    re_ansi_dates = re.compile(r'(?P<year>\d{4})[\\ -]?(?P<month>\d{2})[\\ -]?(?P<day>\d{2})(?:[ T]\d{2}:\d{2}:\d{2})?(?:\.\d+)?(?:[+-]\d{2}:\d{2})?')

    def __init__(self, cls_):
        super().__init__(schedule=Schedule.never)
        self.get_names()
        self.cls_ = cls_

        self.columns = [column.name for column in inspect(self.cls_).c]

        self.errors = {c: {
            'UHL System Number': False,
            'postcode': False,
            'NHS Number': False,
            'Date of Birth': False,
            'Name': set(),
        } for c in self.columns}

    def get_names(self):
        with uhl_dwh_databases_engine() as conn:
            self.names = {r for r, in conn.execute(NAMES_SQL)}

    def do_etl(self):
        count = 0

        with hic_covid_session() as session:
            for x in session.query(self.cls_).yield_per(1000):
                count += 1

                if count % 1000 == 0:
                    self.log(f'{count:,} records checked')

                for c in self.columns:
                    value = getattr(x, c)

                    self.contains_uhl_system_number(c, value)
                    self.contains_postcode(c, value)
                    self.contains_nhs_number(c, value)
                    self.contains_dob(c, value)
                    self.contains_name(c, value)

        self.log(f'{count:,} records checked')

        report = ''
        for c in self.columns:
            if self.errors[c]['UHL System Number']:
                report += f'Column "{c}" may contain a UHL System Number\n'
            if self.errors[c]['postcode']:
                report += f'Column "{c}" may contain a postcode\n'
            if self.errors[c]['NHS Number']:
                report += f'Column "{c}" may contain an NHS Number\n'
            if self.errors[c]['Date of Birth']:
                report += f'Column "{c}" may contain a Date of Birth\n'
            if len(self.errors[c]['Name']) > 0:
                report += f'Column "{c}" may contain the names: {", ".join(self.errors[c]["Name"])}\n'

        self. log(
            message='Validation report',
            attachment=report,
        )


    def contains_name(self, column, value):
        if not value or not isinstance(value, str):
            return

        value = self.re_numbers.sub(' ', value.lower())

        for w in self.re_words.split(value):
            if w in self.names:
                self.errors[column]['Name'].add(w)

    def contains_uhl_system_number(self, column, value):
        if self.errors[column]['UHL System Number']:
            return
        if not value or not isinstance(value, str):
            return

        if self.re_uhl_s_number.search(value):
            self.errors[column]['UHL System Number'] = True

    def contains_postcode(self, column, value):
        if self.errors[column]['postcode']:
            return
        if not value or not isinstance(value, str):
            return

        if self.re_postcodes.search(value):
            self.errors[column]['postcode'] = True

    def contains_nhs_number(self, column, value):
        if self.errors[column]['NHS Number']:
            return
        if not value:
            return

        if isinstance(value, str):
            value = self.re_nhs_dividers.sub('', value)
        else:
            value = str(value)

        # A valid NHS number must be 10 digits long
        matches = self.re_nhs_numbers.findall(value)

        for m in matches:
            if self.calculate_nhs_number_checksum(m) == m[9]:
                self.errors[column]['NHS Number'] = True
                return

    def contains_dob(self, column, value):
        if self.errors[column]['Date of Birth']:
            return

        try:
            dt_val = self.parse_date(value)
        except:
            return

        if not dt_val:
            return

        if (datetime.utcnow().date() - relativedelta(years=130)) < dt_val < (datetime.utcnow().date() - relativedelta(years=10)):
            self.errors[column]['Date of Birth'] = True

    def calculate_nhs_number_checksum(self, nhs_number):
        checkcalc = lambda sum: 11 - (sum % 11)

        char_total = sum(
            [int(j) * (11 - (i + 1)) for i, j in enumerate(nhs_number[:9])]
        )
        return str(checkcalc(char_total)) if checkcalc(char_total) != 11 else '0'

    def parse_date(self, value):
        if not value:
            return None

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, date):
            return value

        value = str(value)

        ansi_match = self.re_ansi_dates.fullmatch(value)

        if ansi_match:
            return date(
                int(ansi_match.group('year')),
                int(ansi_match.group('month')),
                int(ansi_match.group('day')),
            )

        if value.isnumeric():
            # Check that numbers is within a reasonable range
            if not (1_000_000 < int(value) < 100_000_000):
                return None

        parsed_date = parse(value, dayfirst=True)

        return parsed_date.date()


class CovidPpiAdministrationValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Administration)


class CovidPpiBloodTestValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=BloodTest)


class CovidPpiCriticalCarePeriodValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=CriticalCarePeriod)


class CovidPpiDiagnosisValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Diagnosis)


class CovidPpiEpisodeValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Episode)


class CovidPpiMicrobiologyTestValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=MicrobiologyTest)


class CovidPpiObservationValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Observation)


class CovidPpiOrderValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Order)


class CovidPpiPrescribingValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Prescribing)


class CovidPpiProcedureValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Procedure)


class CovidPpiTransferValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Transfer)


class CovidPpiVirologyValidationEtl(CovidPpiValidationEtl):
    def __init__(self):
        super().__init__(cls_=Virology)
