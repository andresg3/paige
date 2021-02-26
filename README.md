Scenario: We received data from a collaborating hospital.  Build an ETL pipeline from the patient_data.csv file.

Background: A blood sugar level less than 140 mg/dL (7.8 mmol/L) is normal. A reading of more than 200 mg/dL (11.1 mmol/L) after two hours indicates diabetes. A reading between 140 and 199 mg/dL (7.8 mmol/L and 11.0 mmol/L) indicates prediabetes

Goal: Store the data in a cleaned and structured format into a database/file of choice. Write the code in Python or language of choice. Design a solution that can be scaled to TB of records.

Steps:

1. Make assumptions and justify them where things are unclear with comments in the code.

2. Write unit tests for all your functions.

3. Write data tests to ensure that the data is correct.

4. Remove Protected health information (PHI): Names, Addresses etc.

5. Clean data. Remove invalid values. Normalize it where reasonable.

6. Add a column that calculates the average of all three glucose measurement time points.

7. Add a column based on the average of all three glucose measurement time points that indicates whether it’s normal, prediabetes or diabetes.

8. Store data in a database or file format of choice.

--

Author: Giovanny A Granda || 
https://github.com/andresg3/simple_etl_paige