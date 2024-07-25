# Event Driven Data Pipeline on Databricks
This project is about a data pipeline built using Spark and Delta lake, tables. Includes setting up Delta tables, integrating data from stage to target zone and creating a workflow on Databricks and Google Cloud Platform (GCP).

## Project Structure
* 'data/': Folder contains orders data
* 'staging_zone_load.ipynb': Script to create stage tables and move files to target directory
* 'target_data_load.ipynb': Script to create target tables and merge both tables based on condition

## Overview
#### Storage
* Raw data is stored on GCS bucket on GCP

#### Defining Delta Tables
* Staging Table: Delta table created from data on GCS bucket
* Target Table: Holds data after applying transformations and

#### Data Processing
* Load data from delta tables and merge them using delta lake's functionality based on matching keys.
* Depending on the condition, it appends new data to the existing data. More like Change Data Capture (CDC)

#### Event Driven Workflow
* Set up workflows in Databricks that are triggered based on a specific event - when new file arrives in GCS bucket
* It then triggers the execution tasks/notebooks from staging to target delta table.
