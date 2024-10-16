# Assessment-Test-Incubytes
This repository will have all test related in python and SQl with TDD
Step 1: TDD-based Plan
Test 1: Verify the creation of staging and country-specific tables.
Test 2: Load sample data and ensure country-based splitting of data works correctly.
Test 3: Add derived columns like age and days_since_last_consulted.
Test 4: Ensure the system picks the latest consultation records for customers.
Test 5: Validate the correctness of data transformations.

Step 2: Data Schema
Staging Table
Country-specific Tables (e.g., Table_India)

Step 3: Python Code Implementation
We'll use Python to simulate the ETL pipeline.

Step 4: Testing (TDD)
We will write tests to validate the transformations and splits

Optimizations for Large Data
Batch Processing: For handling billions of records, implement batch processing by reading and processing chunks of data.
Parallelism: Use Python multiprocessing or distributed frameworks like Spark for parallel processing.
Indexes: Ensure proper indexing on customer IDs and dates in the database for efficient querying.
