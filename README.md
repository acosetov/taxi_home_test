# Taxi Home Task Result

1. Python DAG file name **taxi_home_test_dag.py** 
2. The data quality is described in the file **data_quality_doc.pdf**
3. SQL queries 
 ~~~sql
-- What is the average fare per mile?
SELECT
    SUM(total_amount) /  SUM(trip_distance) AS average_fare_per_mile
FROM  `taxi_dataset.yellow_taxi`;
~~~
~~~sql
-- Which are the 10 pickup taxi zones with the highest average tip?
SELECT
    pickup_latitude, AVG(tip_amount) AS average_tip
FROM
   `taxi_dataset.yellow_taxi`
GROUP BY
    pickup_latitude
ORDER BY average_tip DESC LIMIT 10;
~~~
## Additional information
1. Access to Apache Airflow with working DAG
**url:**http://34.133.246.248:8080
**user:** admin
**password:** 1Dem0Pa$$w0rd!
2. Google Bigquery
**url:** https://console.cloud.google.com/bigquery?project=upbeat-medley-387014&p=upbeat-medley-387014&d=taxi_dataset 
> **Note:** To access the dataset via the provided link, you need to be logged in to your Google account.