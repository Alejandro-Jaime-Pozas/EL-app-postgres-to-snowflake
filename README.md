# EL pipeline

## Overview

The objective of this project is to automate the process of copying data from a source into a data warehouse to allow fast querying of the data and ultimately to extract insights from it (via BI tools not covered in this project).

This EL pipeline automation dag uses the following tools for implementation:

- Orchestrate: Airflow Docker orchestration and scheduling,
- Extract: Neon postgres database source,
- Intermediary Storage: AWS S3 buckets
- Load: Snowflake data warehouse destination


### v1

The initial version of this project will have the following capabilites and limitations.

#### Capabilities
- Can extract data from a single postgres source database
- Can load the extracted data into an s3 storage bucket with a filepath based on table schema and table name
- Can copy the loaded data from s3 into a Snowflake database table with same table schema and table name as original postgres source, keeping all column names and data types as original source (or Snowflake equivalent)
- Can automate this process locally (not in web servers) using Airflow to trigger the entire data pipeline workflow

#### Limitations
- Process does not use parallel loading currently for postgres >> s3 nor s3 >> snowflake
- There is no useful snowflake table metadata to know what file it's coming from or load date
- Process needs to run locally in Docker container, have not implemented web version
- Have not implemented code refactoring
- No exception handling or error checking

The end product for v1 of the project will essentially be a basic EL flow from postgres >> s3 >> snowflake using airflow's built-in connectors/hooks/operators for those services.


# Config

## Data sources and destinations

- Neon Database Postgres contains sample employee data pulled from github
  - Creds: ajaimepozas@gmail.com
  - Location: https://console.neon.tech/app/projects/summer-block-08511227
- AWS s3 account free usage ($100 dls) for 6 months
  - Creds: ajaimepozas@sandiego.edu
  - Location: https://us-east-1.console.aws.amazon.com/console/home?region=us-east-1#
- Snowflake account free usage 30 days ($400 dls)
  - Creds: ajaimepozas@sandiego.edu
  - Location: https://app.snowflake.com/nhhtawd/lg21858/#/homepage

## Project Config

- Using Airflow's Taskflow API for main python code functionality
- Separated requirements files since airflow requires many packages
  - requirements.txt contains bare-bones packages required for dag folder imports
  - requirements.airflow.txt contains all airflow version packages

## Dev Usage

Use
