# MULTINATIONAL-RETAIL-DATA-CENTRALISATION167

## TABLE OF CONTENTS, IF THE README FILE IS LONG

## A DESCRIPTION OF THE PROJECT:

### WHAT IT DOES, THE AIM OF THE PROJECT, AND WHAT YOU LEARNED

## INSTALLATION INSTRUCTIONS

### Requirements

For running this application, it must to be installed:

- python 3.8 or over
- pandas 2.0.2
- sqlalchemy 2.0.30
- tabula and its dependencies
- requests 2.22.0
- boto3 1.34.122

In order to avoid unnecessary errors, create 2 folders in root project:

- data: Should contain a .csv file after you complete the 'TASK 6'.
- config: This folder must have exists 3 yaml (or yml) files with these respectives structure:
  ```
  - config.yaml:
        x-api-key: {your x-api-key}
        aws_url_number_of_store: {your aws_url_number_of_store}
        aws_url_store_details: {your aws_url_store_details}
  - db_creds_local:
        LOCALHOST_HOST: {your localhost address}
        LOCALHOST_PASSWORD: {your localhost password}
        LOCALHOST_USER: {your localhost user name}
        LOCALHOST_DATABASE: {your localhost database name}
        LOCALHOST_PORT: {your localhost port database number}
  - db_creds:
        RDS_HOST: {your rds localhost host}
        RDS_PASSWORD: {your rds password}
        RDS_USER: {your rds user name}
        RDS_DATABASE: {your rds database name}
        RDS_PORT: {your rds port database number}
  ```

## USAGE INSTRUCTIONS

After the packages below has installed, execute this line in root folder:

```

```

## FILE STRUCTURE OF THE PROJECT

```
├── config
│   ├── config.yaml
│   ├── db_creds_local.yaml
│   └── db_creds.yaml
├── data
│   └── products.csv
├── base_data_cleaner.py
├── database_utils.py
├── data_cleaning.py
├── data_extraction.py
├── README.md
```

## LICENSE INFORMATION

##
