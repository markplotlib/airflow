<img src="./img/air.png" alt="airflow logo" width="350" title="airflow logo" align="middle" />

<img src="./img/s3.png" alt="Amazon S3" width="200" title="Amazon S3" align="right" />

# Sparkify Data Pipeline
using Apache Airflow, AWS Redshift and AWS S3

## Project Summary
In this project, I build off of a [previous ETL Pipeline project](https://github.com/markplotlib/sparkify-data-warehouse),
using more automated and better monitored pipelines, primarily through Apache Airflow.
The data draws again from a fictitious music streaming service named Sparkify.

The pipeline channels data from Amazon Web Service's (AWS) Simple Storage Service (S3)
into AWS Redshift data warehouses (in the form of staging tables).
The source datasets consist of JSON logs that tell about user activity in the application
and JSON metadata about the songs the users listen to.

The pipelines are dynamic and built from reusable tasks. They are monitored and allow for easy backfills.
Data quality checks are also automated for analysis execution over the data warehouse,
to catch any discrepancies in the datasets.

### Apache Airflow
In Airflow, I create custom operators to perform tasks that stage the data, fill the data warehouse, and run data quality checks.

<img src="./img/taskflow.png" alt="flow of tasks" width="750" title="flow of tasks" />
