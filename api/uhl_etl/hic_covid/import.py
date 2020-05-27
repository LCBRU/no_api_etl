import _mssql
import logging
import requests
from datetime import timedelta  
from urllib.parse import urlparse, urlunparse, urlencode
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
)


COVID_DEMOGRAPHICS_SQL = '''
SELECT
	replace(p.NHS_NUMBER,' ','') AS nhs_number,
	p.SYSTEM_NUMBER AS uhl_system_number,
	p.CURRENT_GP_PRACTICE AS gp_practice,
	MIN(cv.WHO_AGE_AT_COLLECTION_DATE_YEARS) AS age,
	p.DATE_OF_DEATH AS date_of_death,
	p.Post_Code AS postcode,
	CASE p.Sex
		WHEN 'U' THEN '0'
		WHEN 'M' THEN '1'
		WHEN 'F' THEN '2'
		ELSE '9'
	END sex,
	p.ETHNIC_ORIGIN_CODE ethnic_category
FROM DWMARTS.dbo.PATH_MICRO_COVID_DATA cv
JOIN [DWREPO].[dbo].[PATIENT] p
	ON p.SYSTEM_NUMBER = cv.Hospital_Number
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
		super().__init__(schedule=Schedule.daily_7pm)

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


COVID_VIROLOGY_SQL = '''
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
;
'''


class CovidVirologyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_VIROLOGY_SQL)
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


COVID_EMERGENCY_SQL = '''
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
) AND fpp.ARRIVAL_DATE >= '01-Jan-2020'
;
'''


class CovidEmergencyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_EMERGENCY_SQL)
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


COVID_EPISODE_SQL = '''
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
	SELECT SYSTEM_NUMBER
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
ORDER BY p.SYSTEM_NUMBER, a.ID, ce.EPISODE_NUMBER
;
'''


class CovidEpisodeEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_EPISODE_SQL)
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


COVID_DIAGNOSIS_SQL = '''
SELECT DISTINCT
	a.id AS spell_id,
	ce.ID AS episode_id,
	d.id AS diagnosis_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	d.DIAGNOSIS_NUMBER AS diagnosis_number,
	mf_d.DIAGNOSIS_DESCRIPTION AS diagnosis_code,
	d.DIAGNOSIS_CODE AS diagnosis_name
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
	SELECT SYSTEM_NUMBER
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
;
'''


class CovidDiagnosisEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_DIAGNOSIS_SQL)
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

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving diagnosis batch. Total = {cnt}")
						session.add_all(inserts)
						inserts = []

			session.add_all(inserts)
			session.commit()


COVID_PROCEDURE_SQL = '''
SELECT
	proc_.ID AS procedure_id,
	a.id AS spell_id,
	ce.ID AS episode_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	proc_.PROCEDURE_NUMBER AS procedure_number,
	proc_.PROCEDURE_CODE AS procedure_code,
	opcs.PROCEDURE_DESCRIPTION AS procedure_name
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
	SELECT SYSTEM_NUMBER
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
'''


class CovidProcedureEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_PROCEDURE_SQL)
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

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving procedure batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []

			session.add_all(inserts)
			session.commit()


COVID_TRANSFERS_SQL = '''
SELECT
	t.ID as transfer_id,
	a.id as spell_id,
	p.SYSTEM_NUMBER AS uhl_system_number,
	t.TRANSFER_DATETIME AS transfer_datetime,
	t.FROM_BED AS from_bed,
	from_ward.CODE AS from_ward_code,
	from_ward.WARD AS from_ward_name,
	from_hospital.HOSPITAL AS from_hospital,
	t.TO_BED AS to_bed,
	to_ward.CODE AS to_ward_code,
	to_ward.WARD AS to_ward_name,
	to_hospital.HOSPITAL AS to_hospital
FROM DWREPO.dbo.PATIENT p
JOIN DWREPO.dbo.ADMISSIONS a
	ON a.PATIENT_ID = p.ID
JOIN DWREPO.dbo.TRANSFERS t
	ON t.ADMISSIONS_ID = a.ID
JOIN DWREPO.dbo.MF_WARD from_ward
	ON from_ward.CODE = t.FROM_WARD
	AND from_ward.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_WARD to_ward
	ON to_ward.CODE = t.TO_WARD
	AND to_ward.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_HOSPITAL from_hospital
	ON from_hospital.CODE = t.FROM_HOSPITAL_CODE
	AND from_hospital.LOGICALLY_DELETED_FLAG = 0
JOIN DWREPO.dbo.MF_HOSPITAL to_hospital
	ON to_hospital.CODE = t.TO_HOSPITAL_CODE
	AND to_hospital.LOGICALLY_DELETED_FLAG = 0
WHERE p.SYSTEM_NUMBER IN (
	SELECT SYSTEM_NUMBER
	FROM DWBRICCS.dbo.all_suspected_covid
) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
;
'''


class CovidTransferEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_TRANSFERS_SQL)
				for row in rs:
					v = session.query(Transfer).filter_by(transfer_id=row['transfer_id']).one_or_none()
					if v is None:
						v = Transfer(
							transfer_id=row['transfer_id'],
						)

					v.spell_id=row['spell_id']
					v.uhl_system_number=row['uhl_system_number']
					v.transfer_datetime=row['transfer_datetime']
					v.from_bed=row['from_bed']
					v.from_ward_code=row['from_ward_code']
					v.from_ward_name=row['from_ward_name']
					v.from_hospital=row['from_hospital']
					v.to_bed=row['to_bed']
					v.to_ward_code=row['to_ward_code']
					v.to_ward_name=row['to_ward_name']
					v.to_hospital=row['to_hospital']

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving transfer batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []

			session.add_all(inserts)
			session.commit()


COVID_BLOODS_SQL = '''
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
	CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
		CASE WHEN ISNUMERIC(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1)) = 1 THEN
			CAST(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1) AS DECIMAL(18,5))
		END
	END AS lower_range,
	CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
		CASE WHEN ISNUMERIC(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range))) = 1 THEN
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
WHERE r.WHO_COLLECTION_DATE_TIME >= '01/01/2020 00:0:0'
	AND p.Hospital_Number IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	) AND (
			T.Result_Suppressed_Flag = 'N'
		OR  T.Result_Suppressed_Flag IS NULL
	)
;
'''


class CovidBloodsEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []
		cnt = 0

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_BLOODS_SQL)
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

					inserts.append(v)
					cnt += 1

					if cnt % 1000 == 0:
						logging.info(f"Saving Test batch.  total = {cnt}")
						session.add_all(inserts)
						inserts = []

			session.add_all(inserts)
			session.commit()


COVID_MICROBIOLOGY_SQL = '''
SELECT
	t.id AS test_id,
	p.Hospital_Number AS uhl_system_number,
	o.Lab_Ref_No AS laboratory_code,
	t.Order_code order_code,
	t.Order_Code_Expan order_name,
	t.Test_code test_code,
	tc.Test_Expansion test_name,
	org.Organism organism,
	COALESCE(LTRIM(RTRIM(REPLACE(q.Quantity_Description, '*', ''))), t.Result_Expansion) AS test_result,
	r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
	r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
	t.WHO_TEST_RESULTED_DATE_TIME sample_available_date_time,
	r.specimen_site
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
	r.WHO_COLLECTION_DATE_TIME >= '01/01/2020 00:0:0'
;
'''


class CovidMicrobiologyEtl(Etl):
	def __init__(self):
		super().__init__(schedule=Schedule.daily_7pm)

	def do_etl(self):
		inserts = []

		with hic_covid_session() as session:
			with uhl_dwh_databases_engine() as conn:
				rs = conn.execute(COVID_MICROBIOLOGY_SQL)
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