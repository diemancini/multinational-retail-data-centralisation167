# MULTINATIONAL-RETAIL-DATA-CENTRALISATION167

## TABLE OF CONTENTS, IF THE README FILE IS LONG

## A DESCRIPTION OF THE PROJECT:

### WHAT IT DOES, THE AIM OF THE PROJECT, AND WHAT YOU LEARNED

This project has a lot of features that we've being learning on data engineer course. What it does is read data from different sources,
clean it and save in a safe database.

The main aim and what I've been learned on the project are:

- How to provide a good documentation, using docs strings in python and this README file as well.
- Manage the git repository remotely and locally, which is usefull for controlling versions of your code(saving it, see your progress and comeback in old code if needed, ect).
- Create a script that we can automate the process of manage the data, using built'on, own and third part libraries exists in python. Try to create a clean code and follow good practices are very important concepts that all the enginners have to deal in daily basis life.
- Creating a simple (but not less important) a good structure folders. This could make life of the others enginners so much easier to understand the programm.
- Take the data from different types and sources are very important. In real life time job, we have to manage data from several resources and how to take from:

      - Endpoints via restfull API, which is most used now a days.
      - Read files from cvs, pdf (using tabula as third part library).
      - Read database remotely from cloud system (AWS).

- Setup the postgres tool and database. This part is very important regarding the infra structure, to maintain the system up running without critical problems. I had some issues in order to run properly the server of the database, but I followed the ai core recourses and internet as well, which is explained in INSTALL POSTGRES AND PGADMIN section.
- Manage the data was the most fun part of this application. I've learning how to clean the data using python (pandas, regex, etc) before to save in the database. After that I had to build several sql queries in order to achieve the CRUD concept, using:

      - Drop(delete) tables, constraints, attributes and data.
      - Update or inserting data, constraints and attributes on the tables that I created during the project.
      - Using subqueries, joins, common table expressions in select queries was very fun to do it, to see the results of what I've buit so far.

## INSTALLATION INSTRUCTIONS

### Requirements

#### PYTHON AND GIT

Python has to be installed for running this application. Follow these steps below:

- Install [python 3](https://www.python.org/downloads/) accordingly with your OS.
- Clone repository with

```
git clone https://github.com/diemancini/multinational-retail-data-centralisation167.git
```

#### PACKAGES

For running this application, it must to be installed:

- python 3.8 or over
- pandas 2.0.2
- sqlalchemy 2.0.30
- tabula and its dependencies
- requests 2.22.0
- boto3 1.34.122

#### ADDITIONAL FOLDERS

Git does not allowed empty folders on commit In order to avoid unnecessary errors, CREATE 2 FOLDERS in root project:

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

#### INSTALL POSTGRES AND PGADMIN

You can see the instructions on AICore SQL Essentials classes ([SQL Setup](https://colab.research.google.com/github/AI-Core/Content-Public/blob/main/Content/units/Data-Handling/3.%20SQL/1.%20SQL%20Setup/Notebook.ipynb#scrollTo=-SH39buV5zsJ)) in order how to install and run postgres and pgadmin.

- POSTGRES

However, after installed postgres and running it, could happen some error during execution of postgres sockets and databases.
I find out this solution to sort it out:

1. First of all, it needs to setup the postgres in PATH and PGDATA environment variables in your OS.
   Paste the export syntax at the end of the .bashrc file(located at /home/{username}).

```
export PATH="/usr/lib/postgresql/12/bin/:$PATH"
export PGDATA="/var/lib/postgresql/12/main/data/"
```

The data folder MUST be empty.

2. Save and exit.
3. Execute the script or reboot the system to make the changes live.
4. To verify the changes, run echo:

```
echo $PATH
```

5. Change the owner of the postgres to your user:

```
sudo chown -R diego:diego /var/run/postgresql/
```

6. Run the database server:

```
pg_ctl -l logfile start
```

You should see this message below:

```
waiting for server to start.... done
server started
```

## USAGE INSTRUCTIONS

After the packages above has installed (in requirements section) and the instructions of TASK 1 in MILESTONE 2, now you have to execute the files below:

- create_dim_table.py: it will create dim tables for complete the tasks of MILESTONE 2 and 3. You shoud see the options below:

```
Please, which dim table you want to be created?

0 - ALL
1 - DIM_USERS
2 - DIM_CARD_DETAILS
3 - DIM_STORE_DETAILS
4 - DIM_PRODUCTS
5 - DIM_DATE_TIMES
```

Choose one of this options to create the selected table. Automatically, the orders_table will be created if does not exist.

- queries: show the results of each task of MILESTONE 4.
  In order to execute, execute the create_dim_tables.py script first.
  Now execute this script and another screen options in terminal should appear:

```
Please, which query do you want to be executed?

0 - CLOSE THE PROGRAMM
1 - TOTAL NUMBER OF STORES BY COUNTRY
2 - TOTAL NUMBER STORES BY LOCALITY
3 - MOST SALES PER MONTH
4 - COUNT SALES BY ON OFF LINE
5 - TOTAL SALES BY STORE TYPE
6 - MOST SALES PER MONTH AND YEAR
7 - TOTAL STAFF NUMBERS BY COUNTRY
8 - TOTAL SALES BY STORE TYPE IN GERMANY
9 - AVERAGE TIME TAKEN BETWEEN EACH SALE
```

## FILE STRUCTURE OF THE PROJECT

```
├── config
│   ├── config.yaml
│   ├── db_creds_local.yaml
│   └── db_creds.yaml
├── create_dim_tables.py
├── data
│   └── products.csv
├── database_manager
│   ├── base_database_connector.py
│   ├── base_data_cleaner.py
│   ├── database_utils.py
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   └── __init__.py
├── queries.py
├── README.md
└── sql_queries
    ├── milestone_4_task_1.sql
    ├── milestone_4_task_2.sql
    ├── milestone_4_task_3.sql
    ├── milestone_4_task_4.sql
    ├── milestone_4_task_5.sql
    ├── milestone_4_task_6.sql
    ├── milestone_4_task_7.sql
    ├── milestone_4_task_8.sql
    └── milestone_4_task_9.sql
```

## LICENSE INFORMATION

##
