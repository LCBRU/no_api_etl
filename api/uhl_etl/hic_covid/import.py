import logging
import requests
from datetime import timedelta, datetime
from urllib.parse import urlparse, urlunparse, urlencode
from sqlalchemy import func
from sqlalchemy.sql import text
from api.core import Etl, Schedule
from api.database import uhl_dwh_databases_engine
from api.environment import (
	IDENTITY_API_KEY,
	IDENTITY_HOST,
)
from api.uhl_etl.hic_covid.model import (
	hic_covid_session,
	Demographics,
	Virology,
	Emergency,
	Episode,
	Diagnosis,
	Procedure,
	Transfer,
	BloodTest,
	MicrobiologyTest,
	Prescribing,
	Administration,
	Observation,
	CriticalCarePeriod,
	Order,
)


COVID_DEMOGRAPHICS_SQL = '''
    SELECT
        replace(p.NHS_NUMBER,' ','') AS nhs_number,
        p.SYSTEM_NUMBER AS uhl_system_number,
        p.CURRENT_GP_PRACTICE AS gp_practice,
        DWBRICCS.[dbo].[GetAgeAtDate](MIN(p.PATIENT_DATE_OF_BIRTH), MIN(cv.dateadded)) AS age,
        p.DATE_OF_DEATH AS date_of_death,
        p.Post_Code AS postcode,
        CASE p.Sex
            WHEN 'U' THEN '0'
            WHEN 'M' THEN '1'
            WHEN 'F' THEN '2'
            ELSE '9'
        END sex,
        p.ETHNIC_ORIGIN_CODE ethnic_category
    FROM DWBRICCS.dbo.all_suspected_covid cv
    JOIN [DWREPO].[dbo].[PATIENT] p
        ON p.SYSTEM_NUMBER = cv.uhl_system_number
    GROUP BY
        p.NHS_NUMBER,
        p.SYSTEM_NUMBER,
        p.CURRENT_GP_PRACTICE,
        p.DATE_OF_DEATH,
        p.Post_Code,
        p.Sex,
        p.ETHNIC_ORIGIN_CODE
'''


class CovidParticipantsEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		updates = []

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_DEMOGRAPHICS_SQL)
				for row in rs:
					d = session.query(Demographics).filter_by(uhl_system_number=row['uhl_system_number']).one_or_none()

					if d is None:
						d = Demographics(uhl_system_number=row['uhl_system_number'])
						inserts.append(d)
					else:
						updates.append(d)

					d.nhs_number = row['nhs_number']
					d.gp_practice = row['gp_practice']
					d.age = row['age']
					d.date_of_death = row['date_of_death']
					d.postcode = row['postcode']
					d.sex=row['sex']
					d.ethnic_category=row['ethnic_category']

			if len(inserts) > 0:
				url_parts = urlparse(IDENTITY_HOST)
				url_parts = url_parts._replace(
					query=urlencode({'api_key': IDENTITY_API_KEY}),
					path='api/create_pseudorandom_ids',
				)
				url = urlunparse(url_parts)

				# Do not verify locally signed certificate
				ids = requests.post(url, json={'prefix': 'HCVPt', 'id_count': len(inserts)}, verify=False)

				for id, d in zip(ids.json()['ids'], inserts):
					d.participant_identifier = id

			session.add_all(inserts)
			session.add_all(updates)
			session.commit()


COVID_VIROLOGY_SQL = text('''
SELECT
	t.id AS test_id,
	p.Hospital_Number AS uhl_system_number,
	o.Lab_Ref_No AS laboratory_code,
	t.Order_code order_code,
	t.Order_Code_Expan order_name,
	t.Test_code test_code,
	tc.Test_Expansion test_name,
	org.Organism organism,
	CASE
		WHEN t.Test_code = 'VBIR' THEN LTRIM(RTRIM(REPLACE(q.Quantity_Description, '*', '')))
		ELSE t.Result_Expansion
	END test_result,
	r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
	r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
	t.WHO_TEST_RESULTED_DATE_TIME sample_available_date_time,
	t.Current_Status order_status
FROM DWPATH.dbo.MICRO_TESTS t
INNER JOIN	DWPATH.dbo.MICRO_RESULTS_FILE AS r
	ON t.Micro_Results_File = r.ISRN
INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
	ON r.Order_No = o.Order_Number
INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
	ON o.D_Level_Pointer = p.Request_Patient_Details
LEFT JOIN DWPATH.dbo.MICRO_ORGANISMS org
	ON org.Micro_Tests = t.Micro_Tests
LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_MICRO_WHO tc
	ON t.Test_Code_Key=tc.Test_Codes_Row_ID
LEFT OUTER JOIN DWPATH.dbo.MF_QUANTITY_CODES q
	ON org.Quantifier=q.APEX_ID
LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
	ON o.C_Level_Pointer = s.Request_Source_Details
WHERE
(
	t.Test_code IN  ( 'VCOV', 'VCOV3', 'VCOV4', 'VCOV5' )
	OR (t.Test_code = 'VBIR'AND org.Organism  LIKE  '%CoV%')
) 	AND r.WHO_COLLECTION_DATE_TIME >= '01/01/2020 00:0:0'
    AND r.WHO_RECEIVE_DATE_TIME >= :received_date
;
''')


class CovidVirologyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			max_date = session.query(func.max(Virology.sample_received_date_time)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_VIROLOGY_SQL, received_date=max_date)
				for row in rs:
					if session.query(Virology).filter_by(test_id=row['test_id']).count() == 0:
						v = Virology(
							uhl_system_number=row['uhl_system_number'],
							test_id=row['test_id'],
							laboratory_code=row['laboratory_code'],
							order_code=row['order_code'],
							order_name=row['order_name'],
							test_code=row['test_code'],
							test_name=row['test_name'],
							organism=row['organism'],
							test_result=row['test_result'],
							sample_collected_date_time=['sample_collected_date_time'],
							sample_received_date_time=row['sample_received_date_time'],
							sample_available_date_time=row['sample_available_date_time'],
							order_status=row['order_status'],
						)

						inserts.append(v)

			session.add_all(inserts)
			session.commit()


COVID_EMERGENCY_SQL = text('''
SELECT
	fpp.visitid,
	fpp.PP_IDENTIFIER,
	fpp.ARRIVAL_DATE,
	fpp.ARRIVAL_TIME,
	fpp.DISCHARGE_DATE,
	fpp.DISCHARGE_TIME,
	fpp.PP_ARRIVAL_TRANS_MODE_CODE,
	fpp.PP_ARRIVAL_TRANS_MODE_NAME,
	fpp.PP_PRESENTING_DIAGNOSIS,
	fpp.PP_PRESENTING_PROBLEM,
	fpp.PP_PRESENTING_PROBLEM_CODE,
	fpp.PP_PRESENTING_PROBLEM_NOTES,
	fpp.PP_DEP_DEST_ID,
	fpp.PP_DEP_DEST,
	fpp.PP_DEPARTURE_NATIONAL_CODE 
FROM DWNERVECENTRE.dbo.F_PAT_PRESENT fpp
WHERE fpp.PP_IDENTIFIER in (
	SELECT asc2.UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid asc2
) AND fpp.ARRIVAL_DATE >= :arrival_date
;
''')


class CovidEmergencyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:

			max_date = session.query(func.max(Emergency.arrival_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_EMERGENCY_SQL, arrival_date=max_date)
				for row in rs:
					v = session.query(Emergency).filter_by(visitid=row['visitid']).one_or_none()
					if v is None:
						v = Emergency(
							visitid=row['visitid'],
						)

					v.uhl_system_number=row['PP_IDENTIFIER']
					v.arrival_datetime=self._date_and_time(row['ARRIVAL_DATE'], row['ARRIVAL_TIME'])
					v.departure_datetime=self._date_and_time(row['DISCHARGE_DATE'], row['DISCHARGE_TIME'])
					v.arrival_mode_code=row['PP_ARRIVAL_TRANS_MODE_CODE']
					v.arrival_mode_text=row['PP_ARRIVAL_TRANS_MODE_NAME']
					v.departure_code=row['PP_DEP_DEST_ID']
					v.departure_text=row['PP_DEP_DEST']
					v.complaint_code=row['PP_PRESENTING_PROBLEM_CODE']
					v.complaint_text=row['PP_PRESENTING_PROBLEM']

					inserts.append(v)

			session.add_all(inserts)
			session.commit()

	def _date_and_time(self, date, time):
		if date is None:
			return None

		if not time:
			return date
		
		return date + timedelta(hours=int(time[0:2]), minutes=int(time[2:4]))


COVID_EPISODE_SQL = text('''
SELECT
	ce.ID AS episode_id,
	a.id AS spell_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	a.ADMISSION_DATE_TIME AS admission_datetime,
	a.DISCHARGE_DATE_TIME AS discharge_datetime,
	ROW_NUMBER() OVER (
	    PARTITION BY a.ID
	    ORDER BY ce.CONS_EPISODE_START_DATE_TIME
	) AS order_no_of_episode,
	moa.NC_ADMISSION_METHOD AS admission_method_code,
	moa.NC_ADMISSION_METHOD_NAME AS admission_method_name,
	soa.NC_SOURCE_OF_ADMISSION AS admission_source_code,
	soa.NC_SOURCE_OF_ADMISSION_NAME AS admission_source_name,
	mod_.NC_DISCHARGE_METHOD AS discharge_method_code,
	mod_.NC_DISCHARGE_METHOD_NAME AS discharge_method_name,
	spec.DHSS_CODE AS treatment_function_code,
	spec.NC_SPECIALTY_NAME AS treatment_function_name
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
	ON ce.ADMISSIONS_ID = a.ID
JOIN DWREPO.dbo.MF_METHOD_OF_ADMISSION moa
	ON moa.CODE = a.METHOD_OF_ADMISSION_CODE
	AND moa.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_SOURCE_OF_ADMISSION soa
	ON soa.CODE = a.SOURCE_OF_ADMISSION_CODE
	AND soa.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_METHOD_OF_DISCHARGE mod_
	ON mod_.CODE = a.METHOD_OF_DISCHARGE_CODE
	AND mod_.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_SPECIALTY spec
	ON spec.CODE = ce.SPECIALTY_CODE
	AND spec.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > :admission_datetime
ORDER BY p.SYSTEM_NUMBER, a.ID, ce.EPISODE_NUMBER
;
''')


class CovidEpisodeEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			max_date = session.query(func.max(Episode.admission_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'
			
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_EPISODE_SQL, admission_datetime=max_date)
				for row in rs:
					v = session.query(Episode).filter_by(episode_id=row['episode_id']).one_or_none()
					if v is None:
						v = Episode(
							episode_id=row['episode_id'],
						)

					v.spell_id=row['spell_id']
					v.uhl_system_number=row['uhl_system_number']
					v.admission_datetime=row['admission_datetime']
					v.discharge_datetime=row['discharge_datetime']
					v.order_no_of_episode=row['order_no_of_episode']
					v.admission_method_code=row['admission_method_code']
					v.admission_method_name=row['admission_method_name']
					v.admission_source_code=row['admission_source_code']
					v.admission_source_name=row['admission_source_name']
					v.discharge_method_code=row['discharge_method_code']
					v.discharge_method_name=row['discharge_method_name']
					v.treatment_function_code=row['treatment_function_code']
					v.treatment_function_name=row['treatment_function_name']

					inserts.append(v)

			session.add_all(inserts)
			session.commit()


COVID_DIAGNOSIS_SQL = text('''
SELECT DISTINCT
	a.id AS spell_id,
	ce.ID AS episode_id,
	d.id AS diagnosis_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	d.DIAGNOSIS_NUMBER AS diagnosis_number,
	mf_d.DIAGNOSIS_DESCRIPTION AS diagnosis_name,
	d.DIAGNOSIS_CODE AS diagnosis_code,
	a.ADMISSION_DATE_TIME AS admission_datetime
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
	ON ce.ADMISSIONS_ID = a.ID
JOIN DWREPO.dbo.DIAGNOSES d
	ON d.CONSULTANT_EPISODES_ID = ce.ID
LEFT JOIN DWREPO.dbo.MF_DIAGNOSIS mf_d
	ON mf_d.DIAGNOSIS_CODE = d.DIAGNOSIS_CODE
	AND mf_d.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > :admission_datetime
;
''')


class CovidDiagnosisEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Diagnosis.admission_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_DIAGNOSIS_SQL, admission_datetime=max_date)
				for row in rs:
					v = session.query(Diagnosis).filter_by(diagnosis_id=row['diagnosis_id']).one_or_none()
					if v is None:
						v = Diagnosis(
							diagnosis_id=row['diagnosis_id'],
						)

					v.spell_id=row['spell_id']
					v.episode_id=row['episode_id']
					v.uhl_system_number=row['uhl_system_number']
					v.diagnosis_number=row['diagnosis_number']
					v.diagnosis_code=row['diagnosis_code']
					v.diagnosis_name=row['diagnosis_name']
					v.admission_datetime=row['admission_datetime']

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving diagnosis batch. Total = {cnt}")
						session.add_all(inserts)
						inserts = []
						session.commit()

			session.add_all(inserts)
			session.commit()


COVID_PROCEDURE_SQL = text('''
SELECT
	proc_.ID AS procedure_id,
	a.id AS spell_id,
	ce.ID AS episode_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	proc_.PROCEDURE_NUMBER AS procedure_number,
	proc_.PROCEDURE_CODE AS procedure_code,
	opcs.PROCEDURE_DESCRIPTION AS procedure_name,
	a.ADMISSION_DATE_TIME AS admission_datetime
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
	ON ce.ADMISSIONS_ID = a.ID
JOIN DWREPO.dbo.PROCEDURES proc_
	ON proc_.CONSULTANT_EPISODES_ID = ce.ID
LEFT JOIN DWREPO.dbo.MF_OPCS4 opcs
	ON opcs.PROCEDURE_CODE = proc_.PROCEDURE_CODE
	AND opcs.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > :admission_datetime
;
''')


class CovidProcedureEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Procedure.admission_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_PROCEDURE_SQL, admission_datetime=max_date)
				for row in rs:
					v = session.query(Procedure).filter_by(procedure_id=row['procedure_id']).one_or_none()
					if v is None:
						v = Procedure(
							procedure_id=row['procedure_id'],
						)

					v.spell_id=row['spell_id']
					v.episode_id=row['episode_id']
					v.uhl_system_number=row['uhl_system_number']
					v.procedure_number=row['procedure_number']
					v.procedure_code=row['procedure_code']
					v.procedure_name=row['procedure_name']
					v.admission_datetime=row['admission_datetime']

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving procedure batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []
						session.commit()

			session.add_all(inserts)
			session.commit()


COVID_TRANSFERS_SQL = text('''
SELECT
	a.ID as transfer_id,
	'admission' AS transfer_type,
	a.id as spell_id,
	a.admission_datetime AS transfer_datetime,
	p.SYSTEM_NUMBER AS uhl_system_number,
	ward.CODE AS ward_code,
	ward.WARD AS ward_name,
	hospital.HOSPITAL AS hospital
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
LEFT JOIN DWREPO.dbo.MF_WARD ward
	ON ward.CODE = a.ward_code
	AND ward.LOGICALLY_DELETED_FLAG = 0
LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
	ON hospital.CODE = a.HOSPITAL_CODE
	AND hospital.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
  AND a.admission_datetime > :transfer_datetime

UNION ALL

SELECT
	t.ID as transfer_id,
	'transfer' AS transfer_type,
	a.id as spell_id,
	t.TRANSFER_DATE_TIME AS transfer_datetime,
	p.SYSTEM_NUMBER AS uhl_system_number,
	ward.CODE AS ward_code,
	ward.WARD AS ward_name,
	hospital.HOSPITAL AS hospital
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
JOIN DWREPO.dbo.TRANSFERS t
	ON t.ADMISSIONS_ID = a.ID
LEFT JOIN DWREPO.dbo.MF_WARD ward
	ON ward.CODE = t.TO_WARD
	AND ward.LOGICALLY_DELETED_FLAG = 0
LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
	ON hospital.CODE = t.TO_HOSPITAL_CODE
	AND hospital.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
  AND a.admission_datetime > :transfer_datetime

UNION ALL
  
SELECT
	a.ID as transfer_id,
	'discharge' AS transfer_type,
	a.id as spell_id,
	a.discharge_date_time AS transfer_datetime,
	p.SYSTEM_NUMBER AS uhl_system_number,
	ward.CODE AS ward_code,
	ward.WARD AS ward_name,
	hospital.HOSPITAL AS hospital
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
LEFT JOIN DWREPO.dbo.MF_WARD ward
	ON ward.CODE = a.discharge_ward
	AND ward.LOGICALLY_DELETED_FLAG = 0
LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
	ON hospital.CODE = a.discharge_hospital
	AND hospital.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
  AND a.admission_datetime > :transfer_datetime
;
''')


class CovidTransferEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Transfer.transfer_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_TRANSFERS_SQL, transfer_datetime=max_date)
				for row in rs:
					v = session.query(Transfer).filter_by(transfer_id=row['transfer_id'], transfer_type=row['transfer_type']).one_or_none()
					if v is None:
						v = Transfer(
							transfer_id=row['transfer_id'],
						)

					v.spell_id=row['spell_id']
					v.transfer_type=row['transfer_type']
					v.uhl_system_number=row['uhl_system_number']
					v.transfer_datetime=row['transfer_datetime']
					v.ward_code=row['ward_code']
					v.ward_name=row['ward_name']
					v.hospital=row['hospital']

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving transfer batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []
						session.commit()

			session.add_all(inserts)
			session.commit()


COVID_BLOODS_SQL = text('''
SELECT
	t.id AS test_id,
	p.Hospital_Number AS uhl_system_number,
	tc.Test_Code AS test_code,
	tc.Test_Expansion AS test_name,
	t.[Result] AS result,
	t.Result_Expansion AS result_expansion,
	t.Units AS result_units,
	r.WHO_COLLECTION_DATE_TIME AS sample_collected_datetime,
	t.WHO_RESULTED_DATE_TIME AS result_datetime,
	r.WHO_RECEIVE_DATE_TIME AS receive_datetime,
	CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
		CASE WHEN DWBRICCS.dbo.IsReallyNumeric(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1)) = 1 THEN
			CAST(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1) AS DECIMAL(18,5))
		END
	END AS lower_range,
	CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
		CASE WHEN DWBRICCS.dbo.IsReallyNumeric(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range))) = 1 THEN
			CAST(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range)) AS DECIMAL(18,5))
		END
	END AS higher_range
FROM DWPATH.dbo.HAEM_TESTS t
INNER JOIN	DWPATH.dbo.HAEM_RESULTS_FILE AS r
	ON t.Haem_Results_File = r.ISRN
INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
	ON r.Order_No = o.Order_Number
INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
	ON o.D_Level_Pointer = p.Request_Patient_Details
LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_HAEM_WHO tc
	ON t.Test_Code_Key=tc.Test_Codes_Row_ID
LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
	ON o.C_Level_Pointer = s.Request_Source_Details
WHERE p.Hospital_Number IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	) AND (
			T.Result_Suppressed_Flag = 'N'
		OR  T.Result_Suppressed_Flag IS NULL
	)
	AND r.WHO_RECEIVE_DATE_TIME >= :receive_datetime
;
''')


class CovidBloodsEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(BloodTest.receive_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_BLOODS_SQL, receive_datetime=max_date)
				for row in rs:
					v = session.query(BloodTest).filter_by(test_id=row['test_id']).one_or_none()
					if v is None:
						v = BloodTest(
							test_id=row['test_id'],
						)

					v.uhl_system_number=row['uhl_system_number']
					v.test_code=row['test_code']
					v.test_name=row['test_name']
					v.result=row['result']
					v.result_expansion=row['result_expansion']
					v.result_units=row['result_units']
					v.sample_collected_datetime=row['sample_collected_datetime']
					v.result_datetime=row['result_datetime']
					v.lower_range=row['lower_range']
					v.higher_range=row['higher_range']
					v.receive_datetime=row['receive_datetime']

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving Test batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []
						session.commit()

			session.add_all(inserts)
			session.commit()


COVID_MICROBIOLOGY_SQL = text('''
SELECT
	t.id AS test_id,
	p.Hospital_Number AS uhl_system_number,
	o.Lab_Ref_No AS laboratory_code,
	t.Order_code order_code,
	t.Order_Code_Expan order_name,
	t.Test_code test_code,
	tc.Test_Expansion test_name,
	org.Organism organism,
	COALESCE(org.Quantity_Description, t.Result_Expansion) AS test_result,
	r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
	r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
	t.WHO_TEST_RESULTED_DATE_TIME result_datetime,
	r.specimen_site
FROM DWPATH.dbo.MICRO_TESTS t
INNER JOIN	DWPATH.dbo.MICRO_RESULTS_FILE AS r
	ON t.Micro_Results_File = r.ISRN
INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
	ON r.Order_No = o.Order_Number
INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
	ON o.D_Level_Pointer = p.Request_Patient_Details
LEFT JOIN (
	SELECT
		org.Micro_Tests,
		org.Organism,
		q.Quantity_Description,
		CASE
			WHEN q.Rec_as_Signif_Growth = 'Y' THEN 'Yes'
			ELSE 'No'
		END AS Significant_Growth
	FROM DWPATH.dbo.MICRO_ORGANISMS org
	JOIN DWPATH.dbo.MF_ORGANISM_CODES oc
		ON oc.Organism_code = org.Organism_Code
		AND oc.Organism_category = 'O'
	JOIN DWPATH.dbo.MF_QUANTITY_CODES q
		ON org.Quantifier=q.APEX_ID
) org ON org.Micro_Tests = t.Micro_Tests
LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_MICRO_WHO tc
	ON t.Test_Code_Key=tc.Test_Codes_Row_ID
LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
	ON o.C_Level_Pointer = s.Request_Source_Details
WHERE
	r.WHO_RECEIVE_DATE_TIME >= :sample_received_date_time
	AND	p.Hospital_Number IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	)
;
''')


class CovidMicrobiologyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(MicrobiologyTest.sample_received_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_MICROBIOLOGY_SQL, sample_received_date_time=max_date)
				for row in rs:
					if session.query(MicrobiologyTest).filter_by(test_id=row['test_id']).count() == 0:
						v = MicrobiologyTest(
							uhl_system_number=row['uhl_system_number'],
							test_id=row['test_id'],
							order_code=row['order_code'],
							order_name=row['order_name'],
							test_code=row['test_code'],
							test_name=row['test_name'],
							organism=row['organism'],
							result=row['test_result'],
							sample_collected_datetime=row['sample_collected_date_time'],
							sample_received_datetime=row['sample_received_date_time'],
							result_datetime=row['result_datetime'],
							specimen_site=row['specimen_site'],
						)

						inserts.append(v)
						cnt += 1

						if cnt % 1000 == 0:
							logging.info(f"Saving Test batch.  total = {cnt}")
							session.add_all(inserts)
							inserts = []
							session.commit()

			session.add_all(inserts)
			session.commit()


COVID_PRESCRIBING_SQL = text('''
SELECT
	p.externalId AS uhl_system_number,
	d.id AS order_id,
	m.prescribeMethodName AS method_name,
	m.orderType AS order_type,
	m.medicationName AS medication_name,
	d.minDose AS min_dose,
	d.maxDose AS max_dose,
	freq.frequency_narrative AS frequency,
	form.Name AS form,
	d.doseUnit AS dose_units,
	route.name AS route,
	m.createdOn AS ordered_datetime
FROM DWEPMA.dbo.tciMedication m
JOIN DWEPMA.dbo.tciMedicationDose d
	ON d.medicationid = m.id
JOIN DWEPMA.dbo.tciPerson p
	ON p.id = m.personId
LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FORM_CODES_WHO form
	ON form.CODE = d.formCode
LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FREQUENCY_WHO freq
	ON freq.CODE = d.frequency
LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_ROUTE_OF_ADMINISTRATION_WHO route
	ON route.reference = d.roa
WHERE m.createdOn > :ordered_datetime
	AND p.externalId IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	)
;
''')


class CovidPrescribingEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Prescribing.ordered_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_PRESCRIBING_SQL, ordered_datetime=max_date)
				for row in rs:
					if session.query(Prescribing).filter_by(order_id=row['order_id']).count() == 0:
						v = Prescribing(
							uhl_system_number=row['uhl_system_number'],
							order_id=row['order_id'],
							method_name=row['method_name'],
							order_type=row['order_type'],
							medication_name=row['medication_name'],
							min_dose=row['min_dose'],
							max_does=row['max_dose'],
							frequency=row['frequency'],
							form=['form'],
							does_units=row['dose_units'],
							route=row['route'],
							ordered_datetime=row['ordered_datetime'],
						)

						inserts.append(v)
						cnt += 1

						if cnt % 1000 == 0:
							logging.info(f"Saving medication.  total = {cnt}")
							session.add_all(inserts)
							inserts = []
							session.commit()

			session.add_all(inserts)
			session.commit()


COVID_ADMINISTRATION_SQL = text('''
SELECT
	a.id AS administration_id,
	p.externalId AS uhl_system_number,
	a.eventDateTime AS administration_datetime,
	m.medicationName AS medication_name,
	a.doseId AS dose_id,
	a.dose,
	a.doseUnit AS dose_unit,
	form.Name AS form_name,
	roa.name AS route_name
FROM DWEPMA.dbo.tciAdminEvent a
JOIN DWEPMA.dbo.tciMedication m
	ON m.id = a.medicationid
JOIN DWEPMA.dbo.tciPerson p
	ON p.id = m.personId
LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FORM_CODES_WHO form
	ON form.CODE = a.formCode
LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_ROUTE_OF_ADMINISTRATION_WHO roa
	ON roa.reference = a.roa
WHERE p.externalId IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	) AND a.eventDateTime > :administration_datetime
ORDER BY a.eventDateTime
;
''')


class CovidAdministrationEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Administration.administration_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_ADMINISTRATION_SQL, administration_datetime=max_date)
				for row in rs:
					if session.query(Administration).filter_by(administration_id=row['administration_id']).count() == 0:
						v = Administration(
							uhl_system_number=row['uhl_system_number'],
							administration_id=row['administration_id'],
							administration_datetime=row['administration_datetime'],
							medication_name=row['medication_name'],
							dose_id=row['dose_id'],
							dose=row['dose'],
							dose_unit=row['dose_unit'],
							form_name=row['form_name'],
							route_name=row['route_name'],
						)

						inserts.append(v)
						cnt += 1

						if cnt % 1000 == 0:
							logging.info(f"Saving administration.  total = {cnt}")
							session.add_all(inserts)
							inserts = []
							session.commit()

			session.add_all(inserts)
			session.commit()


COVID_OBSERVATION_SQL = text('''
SELECT *
FROM DWNERVECENTRE.dbo.ObsExport oe
WHERE [System Number > Patient ID] IN (
	SELECT asc2.UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid asc2
) AND oe.Timestamp >= :observation_datetime
ORDER BY  oe.Timestamp
;
''')


class CovidObservationEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Observation.observation_datetime)).scalar() or '01-Jan-2020'
			max_obs_id = int(session.query(func.max(Observation.observation_id)).scalar() or 0)

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_OBSERVATION_SQL, observation_datetime=max_date)

				observation_names = [c[:-4] for c in rs.keys() if c.lower().endswith('_ews') and c[:-4] in rs.keys()]
				ews_names = {c[:-4]: c for c in rs.keys() if c.lower().endswith('_ews')}
				units_names = {c[:-6]: c for c in rs.keys() if c.lower().endswith('_units')}

				observations = [(o, ews_names[o], units_names[o]) for o in observation_names]

				for row in rs:

					observation_id = row['ObsId']
					uhl_system_number = row['System Number > Patient ID']
					observation_datetime = row['Timestamp']

					if observation_id <= max_obs_id:
						if session.query(Observation).filter_by(observation_id=observation_id).count() > 0:
							continue

					for o, ews, units in observations:
						if row[o] is not None or row[ews] is not None:
							v = Observation(
								uhl_system_number=uhl_system_number,
								observation_id=observation_id,
								observation_datetime=observation_datetime,
								observation_name=o,
								observation_value=row[o],
								observation_ews=row[ews],
								observation_units=row[units],
							)

							inserts.append(v)
							cnt += 1

							if cnt % 1000 == 0:
								logging.info(f"Saving observation.  total = {cnt}")
								session.add_all(inserts)
								session.commit()
								inserts = []

			session.add_all(inserts)
			session.commit()


COVID_CLINICAL_CARE_PERIOD_SQL = text('''
SELECT
	ccp.ID AS ccp_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	ccp.CCP_LOCAL_IDENTIFIER,
	spec.DHSS_CODE AS treatment_function_code,
	spec.NC_SPECIALTY_NAME AS treatment_function_name,
	ccp.CCP_START_DATE_TIME,
	loc.LOCATION,
	BASIC_RESP_LEVEL_DAYS AS BASIC_RESPIRATORY_SUPPORT_DAYS,
	ADVANCED_RESP_LEVEL_DAYS AS ADVANCED_RESPIRATORY_SUPPORT_DAYS,
	BASIC_CARDIO_LEVEL_DAYS AS BASIC_CARDIOVASCULAR_SUPPORT_DAYS,
	ADVANCED_CARDIO_LEVEL_DAYS AS ADVANCED_CARDIOVASCULAR_SUPPORT_DAYS,
	RENAL_SUPPORT_DAYS AS RENAL_SUPPORT_DAYS,
	NEURO_SUPPORT_DAYS AS NEUROLOGICAL_SUPPORT_DAYS,
	DERM_SUPPORT_DAYS AS DERMATOLOGICAL_SUPPORT_DAYS,
	LIVER_SUPPORT_DAYS AS LIVER_SUPPORT_DAYS,
	CRITICAL_CARE_LEVEL2_DAYS AS CRITICAL_CARE_LEVEL_2_DAYS,
	CRITICAL_CARE_LEVEL3_DAYS AS CRITICAL_CARE_LEVEL_3_DAYS,
	ccp.CCP_END_DATE_TIME
FROM DWREPO_BASE.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS ccp
JOIN DWREPO.dbo.PATIENT p
	ON p.ID = ccp.PATIENT_ID
JOIN DWREPO.dbo.MF_SPECIALTY spec
	ON spec.CODE = ccp.CCP_TREATMENT_FUNCTION_CODE
JOIN DWREPO.dbo.MF_LOCATION_WHO loc
	ON loc.code = ccp.CCP_LOCATION_CODE
WHERE ccp.CCP_START_DATE >= '01 Jan 2020'
	AND p.SYSTEM_NUMBER IN (
		SELECT UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid
	) AND ccp.CCP_START_DATE_TIME >= :start_datetime
;
''')


class CovidClincalCarePeriodEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(CriticalCarePeriod.start_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_CLINICAL_CARE_PERIOD_SQL, start_datetime=max_date)
				for row in rs:
					if session.query(CriticalCarePeriod).filter_by(ccp_id=row['ccp_id']).count() == 0:
						v = CriticalCarePeriod(
							uhl_system_number=row['uhl_system_number'],
							ccp_id=row['ccp_id'],
							local_identifier=row['CCP_LOCAL_IDENTIFIER'],
							treatment_function_code=row['treatment_function_code'],
							treatment_function_name=row['treatment_function_name'],
							start_datetime=row['CCP_START_DATE_TIME'],
							basic_respiratory_support_days=row['BASIC_RESPIRATORY_SUPPORT_DAYS'],
							advanced_respiratory_support_days=row['ADVANCED_RESPIRATORY_SUPPORT_DAYS'],
							basic_cardiovascular_support_days=row['BASIC_CARDIOVASCULAR_SUPPORT_DAYS'],
							advanced_cardiovascular_support_days=row['ADVANCED_CARDIOVASCULAR_SUPPORT_DAYS'],
							renal_support_days=row['RENAL_SUPPORT_DAYS'],
							neurological_support_days=row['NEUROLOGICAL_SUPPORT_DAYS'],
							dermatological_support_days=row['DERMATOLOGICAL_SUPPORT_DAYS'],
							liver_support_days=row['LIVER_SUPPORT_DAYS'],
							critical_care_level_2_days=row['CRITICAL_CARE_LEVEL_2_DAYS'],
							critical_care_level_3_days=row['CRITICAL_CARE_LEVEL_3_DAYS'],
							discharge_datetime=row['CCP_END_DATE_TIME'],
						)

						inserts.append(v)
						cnt += 1

						if cnt % 1000 == 0:
							logging.info(f"Saving clinical care period.  total = {cnt}")
							session.add_all(inserts)
							inserts = []
							session.commit()

			session.add_all(inserts)
			session.commit()


COVID_ORDERS_SQL = text('''
SELECT
	his.HIS_ID AS uhl_system_number,
	o.ORDER_ID,
	o.ORDER_KEY,
	o.SCHEDULE_DATE,
	ev.Request_Date_Time,
	o.EXAMINATION AS examination_code,
	exam_cd.NAME AS examination_description,
	SUBSTRING(exam_cd.SNOMEDCT, 1, CHARINDEX(',', exam_cd.SNOMEDCT + ',') - 1) AS snomed_code,
	modality.MODALITY
FROM DWRAD.dbo.CRIS_EXAMS_TBL exam
JOIN DWRAD.dbo.CRIS_EVENTS_TBL ev
	ON ev.EVENT_KEY = exam.EVENT_KEY
JOIN DWRAD.dbo.CRIS_EXAMCD_TBL exam_cd
	ON exam_cd.CODE = exam.EXAMINATION
JOIN DWRAD.dbo.CRIS_ORDERS_TBL o
	ON o.EXAM_KEY = exam.EXAM_KEY
JOIN DWRAD.dbo.CRIS_HIS_TBL his
	ON his.PASLINK_KEY = o.PASLINK_KEY
JOIN DWRAD.dbo.MF_CRISMODL modality
	ON modality.CODE = exam_cd.MODALITY
WHERE ev.Request_Date_Time >= :request_datetime
	AND ev.Request_Date_Time < :current_datetime
	AND his.HIS_ID in (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	)
;
''')


class OrdersEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_9_30pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			max_date = session.query(func.max(Order.request_datetime)).scalar()
			max_date = max_date or '01-Jan-2020'

			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_ORDERS_SQL, request_datetime=max_date, current_datetime=datetime.utcnow())
				for row in rs:
					if session.query(Order).filter_by(order_id=row['ORDER_ID']).count() == 0:
						v = Order(
							uhl_system_number=row['uhl_system_number'],
							order_id=row['ORDER_ID'],
							order_key=row['ORDER_KEY'],
							scheduled_datetime=row['SCHEDULE_DATE'],
							request_datetime=row['Request_Date_Time'],
							examination_code=row['examination_code'],
							examination_description=row['examination_description'],
							snomed_code=row['snomed_code'],
							modality=row['MODALITY'],
						)

						inserts.append(v)
						cnt += 1

						if cnt % 1000 == 0:
							logging.info(f"Saving order.  total = {cnt}")
							session.add_all(inserts)
							session.commit()
							inserts = []

			session.add_all(inserts)
			session.commit()
