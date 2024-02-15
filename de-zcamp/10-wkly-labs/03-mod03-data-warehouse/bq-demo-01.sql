-- Delete and Recreate
DROP TABLE shamb0-zoomcamp-lab-01.nycdset.exttbl_green_2022_tripdata;

-- Setting Up an External Table Linked to a Google Cloud Storage Path
CREATE OR REPLACE EXTERNAL TABLE shamb0-zoomcamp-lab-01.nycdset.new_exttbl_green_2022_tripdata OPTIONS (
    format = 'PARQUET',
    uris = ['gs://shamb0_zcamp_2024_hcl_demo_v1_bucket/green_taxi/*']
);

-- Generating a Standard Table from an External Data Source
CREATE OR REPLACE TABLE shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_nonpartitioned AS
SELECT *
FROM shamb0-zoomcamp-lab-01.nycdset.new_exttbl_green_2022_tripdata;

-- Question 1
SELECT COUNT(*)
FROM shamb0-zoomcamp-lab-01.nycdset.new_exttbl_green_2022_tripdata;

-- Question 2
-- External table
SELECT COUNT(DISTINCT(PULocationID))
FROM shamb0-zoomcamp-lab-01.nycdset.new_exttbl_green_2022_tripdata;

-- Internal table
SELECT COUNT(DISTINCT(PULocationID))
FROM shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_nonpartitioned;

-- Question 3
SELECT COUNT(*)
FROM shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_nonpartitioned
WHERE fare_amount = 0;

-- Question 4
CREATE OR REPLACE TABLE shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_partitioned PARTITION BY DATE(lpep_pickup_datetime) CLUSTER BY PUlocationID AS (
        SELECT *
        FROM shamb0-zoomcamp-lab-01.nycdset.new_exttbl_green_2022_tripdata
    );

-- Question 5
SELECT DISTINCT(PULocationID)
FROM shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_nonpartitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
SELECT DISTINCT(PULocationID)
FROM shamb0-zoomcamp-lab-01.nycdset.green_tripdata_2022_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';