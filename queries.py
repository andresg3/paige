create_lab_schema = """CREATE SCHEMA IF NOT EXISTS lab;"""

create_blood_work_table = """
    CREATE TABLE IF NOT EXISTS lab.blood_work (
	patient_id bigint PRIMARY KEY,
	reading_1 float,
	reading_2 float,
	reading_3 float,
	cancer_present varchar,
	atrophy_present int,
	readings_avg float, 
	levels varchar
);
"""

insert_blood_work_table = """INSERT INTO lab.blood_work VALUES """