-- Create an external table from parquet file
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412717.nytaxi.nyc_external_green_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-illia/nyc_green_taxi.parquet']
);

-- Create a table based on external table
CREATE OR REPLACE TABLE terraform-demo-412717.nytaxi.nyc_non_partitioned_green_taxi AS
SELECT * FROM terraform-demo-412717.nytaxi.nyc_external_green_taxi;

-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT count(*) FROM terraform-demo-412717.nytaxi.nyc_non_partitioned_green_taxi;
-- 840402

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT DISTINCT count('PULocationIDs') from terraform-demo-412717.nytaxi.nyc_external_green_taxi;
SELECT DISTINCT count('PULocationIDs') from terraform-demo-412717.nytaxi.nyc_non_partitioned_green_taxi;
-- 0 MB for the External Table and 0MB for the Materialized Table


-- Question 3: How many records have a fare_amount of 0
SELECT count(*) FROM terraform-demo-412717.nytaxi.nyc_external_green_taxi WHERE fare_amount = 0
-- 1,622

-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime?
-- Partition by lpep_pickup_datetime Cluster on PUlocationID

CREATE OR REPLACE TABLE 'terraform-demo-412717.nytaxi.nyc_partitioned_clustered_green_taxi'
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM `terraform-demo-412717.nytaxi.nyc_external_green_taxi`;



-- Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?


SELECT DISTINCT PULocationID 
FROM terraform-demo-412717.nytaxi.nyc_partitioned_clustered_green_taxi WHERE cleaned_pickup_datetime BETWEEN TIMESTAMP '2022-06-01 00:00:00' AND TIMESTAMP '2022-06-30 23:59:59';

SELECT DISTINCT PULocationID
FROM terraform-demo-412717.nytaxi.nyc_non_partitioned_green_taxi 
WHERE TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) BETWEEN TIMESTAMP '2022-06-01 00:00:00' AND TIMESTAMP '2022-06-30 23:59:59';

-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table



