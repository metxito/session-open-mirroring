## 01_basic_example

Only few csv examples that you can upload directly into OneLake

## 02_automatic_simple_mirroring

This demo is a simple version a python file that you can use to learn how to create .parquet from a panda dataset and how to upload files into OneLake LandingZone.

Steps:

- Create a copy of 00_config_template.json as 00_config.json and fill in with the necessary information
- Run this script to only create a _metadata.json and a 0000000000000000000000001.parquet file in the /results/2_automatic_simple_mirroring folder
```
    python 01_create_files.py
```
- Run this script to only create a _metadata.json and a 0000000000000000000000001.parquet file for many tables. The will be stored in the folder /results/2_automatic_simple_mirroring
```
    python 02_create_and_upload_files.py
```
- Open a new console and execute this python script. This will simulate new entries time to time.
```
    python 03_simulating_new_entries.py
```
- In the first console, and in parallel together the previous step, execute this python script. This will detect changes and upload them.
```
    python 03_detect_and_upload_changes.py
```



## 03_test_cdc

Small .sql file you learn how to enable CDC

## 04_cdc_mirroring_app

A full python web application that show you how to manage the different CDC objects from a SQL source system.
Also you can extend the queries or restart the complete mirroring of a specific table.

You start the app you can run the script: bash start-webapp.sh in the 04_cdc_mirroring_app subfolder

When the app is runing you browse the bellows urls:
- http://localhost:9080/current_status that show the current status of the table mirroing configurations
- http://localhost:9080/simulation a simple web from where you can start a "transactional" simulation that will inserts records in the transactions and payments tables.

