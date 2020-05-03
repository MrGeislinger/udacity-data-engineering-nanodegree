# Data Pipeline Airflow Example Project

## Summary

This project from Udacity's Data Engineering Nanodegree to demonstrate how to automate and monitor a ETL pipelines using Apache Airflow.

## The Scenario

We're working with a music streaming startup that has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

We are tasked to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Files

### DAG

* `dags/udac_example_dag.py` holds tasks and dependencies for the DAG.

### Operators

> These scripts will be used in the tasks for the DAG.

* `plugins/operators/stage_redshift.py` holds `StageToRedShiftOperator` necessary to load the JSON data to Redshift's staging tables.
* `plugins/operators/load_fact.py` holds `LoadFactOperator` to insert data from the staging tables into the fact table.
* `plugins/operators/load_dimension.py` holds `LoadDimensionOperator` to insert data from the staging tables to the dimension table.
* `plugins/operators/data_quality.py` holds `DataQualityOperator` to run quality checks on the data using a test query and the expected test results.

### SQL Queries

* `dags/create_tables.sql` holds queries to create tables for the Redshift data warehouse.
* `plugins/helpers/sql_queries.sql` holds queries for inserts and selects called for the respective tasks.

## Airflow Connections

Ensure you add AWS credentials (`aws_credentials`) & connection to the Redshift database (`redshift`) as Airflow Connections.

