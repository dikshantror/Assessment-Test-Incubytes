# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE staging_table (
# MAGIC     customer_name VARCHAR(255),
# MAGIC     customer_id VARCHAR(18),
# MAGIC     open_date DATE,
# MAGIC     last_consulted_date DATE,
# MAGIC     vaccination_id CHAR(5),
# MAGIC     doctor_name VARCHAR(255),
# MAGIC     state CHAR(5),
# MAGIC     country CHAR(5),
# MAGIC     date_of_birth DATE,
# MAGIC     is_active CHAR(1)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Table_India (
# MAGIC     customer_name VARCHAR(255),
# MAGIC     customer_id VARCHAR(18),
# MAGIC     open_date DATE,
# MAGIC     last_consulted_date DATE,
# MAGIC     vaccination_id CHAR(5),
# MAGIC     doctor_name VARCHAR(255),
# MAGIC     state CHAR(5),
# MAGIC     country CHAR(5),
# MAGIC     date_of_birth DATE,
# MAGIC     is_active CHAR(1),
# MAGIC     age INT,
# MAGIC     days_since_last_consulted INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  Table_India;

# COMMAND ----------

import pandas as pd
from datetime import datetime

# Sample data - This mimics data loaded into staging
data = {
    'customer_name': ['Alex', 'John', 'Mathew', 'Matt', 'Jacob'],
    'customer_id': [123457, 123458, 123459, 12345, 1256],
    'open_date': ['20101012', '20101012', '20101012', '20101012', '20101012'],
    'last_consulted_date': ['20121013', '20121013', '20121013', '20121013', '20121013'],
    'vaccination_id': ['MVD', 'MVD', 'MVD', 'MVD', 'MVD'],
    'doctor_name': ['Paul', 'Paul', 'Paul', 'Paul', 'Paul'],
    'state': ['SA', 'TN', 'WAS', 'BOS', 'VIC'],
    'country': ['USA', 'IND', 'PHIL', 'NYC', 'AU'],
    'date_of_birth': ['19870306', '19870306', '19870306', '19870306', '19870306'],
    'is_active': ['A', 'A', 'A', 'A', 'A']
}

# Convert data into a DataFrame
df_staging = pd.DataFrame(data)

# Convert date columns from string to datetime format
df_staging['open_date'] = pd.to_datetime(df_staging['open_date'], format='%Y%m%d')
df_staging['last_consulted_date'] = pd.to_datetime(df_staging['last_consulted_date'], format='%Y%m%d')
df_staging['date_of_birth'] = pd.to_datetime(df_staging['date_of_birth'], format='%Y%m%d')

# Step 4: Add derived columns

# Calculate age
current_date = datetime.now()
df_staging['age'] = df_staging['date_of_birth'].apply(lambda dob: current_date.year - dob.year - ((current_date.month, current_date.day) < (dob.month, dob.day)))

# Calculate days since last consulted
df_staging['days_since_last_consulted'] = df_staging['last_consulted_date'].apply(lambda x: (current_date - x).days)

# Step 5: Split the data by country
country_groups = df_staging.groupby('country')

# Dictionary to hold country-specific dataframes
country_dfs = {}

for country, group in country_groups:
    country_dfs[country] = group

# Sample to print the Indian customer data (Table_India)
print(country_dfs['IND'])

# Assuming we have validation to check if consultation is older than 30 days
df_filtered = df_staging[df_staging['days_since_last_consulted'] > 30]

# Sample output
print(df_filtered)


# COMMAND ----------

import unittest
from datetime import datetime
from dateutil.relativedelta import relativedelta

class HospitalETL:
    def __init__(self, column_mapping, derived_columns):
        """
        Initialize the ETL process with dynamic column mappings and derived column calculations.
        
        Args:
        column_mapping (dict): A mapping of required column names (e.g. customer_id, dob) to actual dataset column names.
        derived_columns (list): A list of dynamic derived columns to calculate with their respective functions.
        """
        self.tables = {}
        self.column_mapping = column_mapping
        self.derived_columns = derived_columns

    def load_data(self, data):
        """
        Load the data into corresponding country-specific tables and apply derived columns.
        """
        for record in data:
            country_col = self.column_mapping['country']
            country = record[country_col]
            
            if country not in self.tables:
                self.tables[country] = []
            
            # Get customer identifier column dynamically
            customer_id_col = self.column_mapping['customer_id']
            customer_id = record[customer_id_col]
            
            # Check if record for this customer already exists and if so, update with the latest consultation
            existing_record = next((r for r in self.tables[country] if r[customer_id_col] == customer_id), None)
            last_consulted_col = self.column_mapping['last_consulted_date']
            if existing_record is None or record[last_consulted_col] > existing_record[last_consulted_col]:
                self.tables[country] = [r for r in self.tables[country] if r[customer_id_col] != customer_id]
                self.tables[country].append(record)
        
        # Apply derived columns dynamically
        for country, records in self.tables.items():
            for record in records:
                for derived_col_name, derived_func in self.derived_columns:
                    record[derived_col_name] = derived_func(record)

    def calculate_age(self, dob):
        dob_date = datetime.strptime(dob, "%Y%m%d")
        today = datetime.today()
        return relativedelta(today, dob_date).years

    def days_since_last_consulted(self, last_consulted_date):
        consult_date = datetime.strptime(last_consulted_date, "%Y%m%d")
        today = datetime.today()
        delta = today - consult_date
        return delta.days


# Step 2: Define dynamic derived columns and test cases
class TestHospitalETL(unittest.TestCase):
    def setUp(self):
        # Column mappings
        self.column_mapping = {
            'customer_id': 'Customer_Id',
            'dob': 'DOB',
            'last_consulted_date': 'Last_Consulted_Date',
            'country': 'Country'
        }
        
        # Derived column functions
        self.derived_columns = [
            ('Age', lambda record: self.etl.calculate_age(record[self.column_mapping['dob']])),
            ('Days_Since_Last_Consulted', lambda record: self.etl.days_since_last_consulted(record[self.column_mapping['last_consulted_date']]))
        ]
        
        self.etl = HospitalETL(self.column_mapping, self.derived_columns)

        # Sample data
        self.sample_data = [
            {'Customer_Name': 'Alex', 'Customer_Id': '123457', 'Open_Date': '20101012', 'Last_Consulted_Date': '20121013', 
             'Vaccination_Id': 'MVD', 'Dr_Name': 'Paul', 'State': 'SA', 'Country': 'USA', 'DOB': '19870306', 'Is_Active': 'A'},
            {'Customer_Name': 'John', 'Customer_Id': '123458', 'Open_Date': '20101012', 'Last_Consulted_Date': '20121013', 
             'Vaccination_Id': 'MVD', 'Dr_Name': 'Paul', 'State': 'TN', 'Country': 'IND', 'DOB': '19870306', 'Is_Active': 'A'},
            {'Customer_Name': 'Jacob', 'Customer_Id': '1256', 'Open_Date': '20101012', 'Last_Consulted_Date': '20121013', 
             'Vaccination_Id': 'MVD', 'Dr_Name': 'Paul', 'State': 'VIC', 'Country': 'AU', 'DOB': '19870306', 'Is_Active': 'A'}
        ]

    def test_load_data(self):
        self.etl.load_data(self.sample_data)
        
        # Assert country-specific tables were created
        self.assertIn('USA', self.etl.tables)
        self.assertIn('IND', self.etl.tables)
        self.assertIn('AU', self.etl.tables)
        
        # Assert data was loaded correctly
        self.assertEqual(len(self.etl.tables['USA']), 1)
        self.assertEqual(self.etl.tables['USA'][0]['Customer_Name'], 'Alex')

    def test_derived_columns(self):
        self.etl.load_data(self.sample_data)

        # Check if Age and Days_Since_Last_Consulted columns were added dynamically
        usa_record = self.etl.tables['USA'][0]
        self.assertIn('Age', usa_record)
        self.assertIn('Days_Since_Last_Consulted', usa_record)

        # Check the dynamically calculated values
        expected_age = self.etl.calculate_age('19870306')
        expected_days = self.etl.days_since_last_consulted('20121013')
        self.assertEqual(usa_record['Age'], expected_age)
        self.assertEqual(usa_record['Days_Since_Last_Consulted'], expected_days)

    def test_latest_consultation(self):
        # Adding the same customer with an older consultation date
        self.etl.load_data(self.sample_data)
        updated_data = [{'Customer_Name': 'Alex', 'Customer_Id': '123457', 'Open_Date': '20101012', 'Last_Consulted_Date': '20111012',
                         'Vaccination_Id': 'MVD', 'Dr_Name': 'Paul', 'State': 'SA', 'Country': 'USA', 'DOB': '19870306', 'Is_Active': 'A'}]
        self.etl.load_data(updated_data)

        # The record should not be updated since it's an older consultation
        usa_record = self.etl.tables['USA'][0]
        self.assertEqual(usa_record['Last_Consulted_Date'], '20121013')

    def test_update_consultation(self):
        # Adding the same customer with a newer consultation date
        self.etl.load_data(self.sample_data)
        updated_data = [{'Customer_Name': 'Alex', 'Customer_Id': '123457', 'Open_Date': '20101012', 'Last_Consulted_Date': '20131013',
                         'Vaccination_Id': 'MVD', 'Dr_Name': 'Paul', 'State': 'SA', 'Country': 'USA', 'DOB': '19870306', 'Is_Active': 'A'}]
        self.etl.load_data(updated_data)

        # The record should be updated with the new consultation date
        usa_record = self.etl.tables['USA'][0]
        self.assertEqual(usa_record['Last_Consulted_Date'], '20131013')


# Running the tests
if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

