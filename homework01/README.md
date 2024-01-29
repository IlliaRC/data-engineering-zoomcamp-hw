# Homework 01

**Question 1**

`(base) user@LM15UQ05N bend % docker run --help`
 
**--rm     Automatically remove the container when it exits**


**Question 2**

`(base) user@LM15UQ05N bend % docker run -it --entrypoint=bash python:3.9 
root@bb846b7e00f5:/# pip list`

**wheel   0.42.0**


**Prepare Postgres data**

`(base) user@LM15UQ05N dataCamp % docker build -t taxi_ingest:v001 .
(base) user@LM15UQ05N dataCamp % docker run -it taxi_ingest:v001 --user=root --password=root
--host=192.168.0.102 --port=5432 --db=ny_taxi --trips_table_name=green_taxi_trips 
--trips_url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz 
--zones_table_name=green_taxi_zones --zones_url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
`

**Question 3. Count records**

`SELECT COUNT(*)
FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = '2019-09-18' AND DATE(lpep_dropoff_datetime) = '2019-09-18';`

**15612**

**Question 4. Largest trip for each day**

`SELECT DATE(lpep_pickup_datetime) AS pickup_day,
       MAX(trip_distance) AS max_trip_distance
FROM green_taxi_trips
GROUP BY pickup_day ORDER BY max_trip_distance DESC LIMIT 1;`

**"2019-09-26" 341.64**


**Question 5. Three biggest pick up Boroughs**

`SELECT
    z."Borough" AS pickup_borough,
    SUM(t."total_amount") AS total_amount_sum
FROM
    green_taxi_trips t
JOIN
    green_taxi_zones z ON t."PULocationID" = z."LocationID"
WHERE
    DATE(t."lpep_pickup_datetime") = '2019-09-18'
    AND z."Borough" <> 'Unknown'
GROUP BY
    z."Borough"
HAVING
    SUM(t."total_amount") > 50000
ORDER BY
    total_amount_sum DESC
LIMIT 3;`

**"Brooklyn"	
"Manhattan"
"Queens"**

Question 6. Largest tip

`SELECT
    z_dropoff."Zone" AS dropoff_zone_name,
    MAX(t."tip_amount") AS max_tip_amount
FROM
    green_taxi_trips t
JOIN
    green_taxi_zones z_pickup ON t."PULocationID" = z_pickup."LocationID"
JOIN
    green_taxi_zones z_dropoff ON t."DOLocationID" = z_dropoff."LocationID"
WHERE
    DATE(t."lpep_pickup_datetime") >= '2019-09-01'
    AND DATE(t."lpep_pickup_datetime") < '2019-10-01'
    AND z_pickup."Zone" = 'Astoria'
GROUP BY
    dropoff_zone_name
ORDER BY
    max_tip_amount DESC
LIMIT 1;`

**"JFK Airport"	62.31**

**Question 7. Creating Resources**

Shortened output

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/terraform-demo-412717/datasets/demo_terraform_dataset]
google_storage_bucket.demo-bucket: Creation complete after 2s [id=demo-terraform-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.






