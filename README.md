# Totesys

<!-- Brief description in language that both technical and non-technical people will understand -->

Totesys is a data platform that extracts data from an operational database, transforms it into a star schema format, archives it in a data lake, and makes it available in a remodelled OLAP data warehouse. 

This is a final project for a Data Engineering bootcamp, and aims to demonstrate knowledge of an end-to-end data engineering pipeline, good operational processes, and Agile working. 


## Table of contents 

- [Installation](#installation)
- [Usage](#usage)
- [How it works](#how-it-works)
- [Contributors](#contributors)

## Installation

<!-- How to install or use the project locally -->

### Prerequisites

* Python - Please refer to the [installation guide](https://www.python.org/downloads/) for instructions on how to install Python. If you are on MacOS and using homebrew, you can install it using the following command: 
  ```
  brew install python
  ```

* Terraform - [Installation guide](https://developer.hashicorp.com/terraform/install)
  ```
  brew tap hashicorp/tap
  brew install hashicorp/tap/terraform
  ```

* AWS - [An account is required](https://aws.amazon.com/) to host the data platform

* Access to a source database with operational data, set up according to the source tables listed in the [how it works](#how-it-works) section

* Access to a Data Warehouse to store the transformed data, set up according to the star schema in the [how it works](#how-it-works) section

* Additional requirements as listed in requirements.txt: 
  * bandit
  * black
  * boto3
  * botocore
  * coverage
  * moto
  * pandas
  * pg8000
  * pyarrow
  * pytest
  * pytest-coverage
  * requests
  * responses

### Guide

**Step 1: Clone the repo**

```
git clone https://github.com/Achint-T/totes-infrastructure.git
```

**Step 2: Update credentials**

* **Github secrets manager**: 
  * Store the following credentials for your AWS account in GitHub secrets manager:
    * AWS Console Access Key - name this secret `AWS_ACCESS_KEY_ID`
    * Secret Access Key - name this secret `AWS_SECRET_ACCESS_KEY`
    * Region - name this secret `AWS_REGION`
  * See [guide](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions) to using GitHub secrets

* **AWS secrets manager**: Store the credentials for the source database in the following format in AWS secrets manager: 
  * TO BE ADDED <!-- Please add this -->


**Step 3: Set up virtual environment, install dependencies and set pythonpath**
```
make requirements
```
```
source venv/bin/activate
```
```
export PYTHONPATH=$(pwd)
```

**Step 4: Push to GitHub**

<!-- How do we get the CI/CD pipeline to apply the code, if they've opened the file for the first time and haven't made any changes yet? -->


## Usage

<!-- Examples of how this project can be used -->

This project can be modified to ingest data from a range of source databases, and transform and output this data into a Data Warehouse. 

It is suitable for use cases where the operational data is regularly updated and will need to be refresed in the data warehouse every 30 min. 

The key files to be modified are:
* src/transform_utils: Each python file corresponds to a table in the data warehouse star schema. These files can be modified to fit different source tables and star schemas. 
* test: The tests will need to be updated to align with any changes made to the python functions. 


## How it works  

### Technologies used

* Python
* SQL
* AWS
* Terraform
* YAML
<!-- Add data visualisation technology once confirmed -->


### Process

<!-- Add diagrams here -->

1. The **ingestion lambda** (*Python and SQL*) continually ingests tables from the operational database (`totesys`). It runs automatically on a 5 min schedule, with any changes to the source database since the last run picked up for ingestion.   

2. The **ingestion S3 bucket** contains the ingested raw data, timestamped and stored in CSV format. 

3. The **transform lambda** (*Python*) is automatically triggered by the ingestion of new files into the ingestion S3 bucket. It uses a number of util functions to pick up new CSV files from the ingestion S3 bucket and transform the data (using *Pandas Dataframes*) to match the fact and dimension tables in the end-state star schema. Finally, it converts the data into *Parquet files* for storage. 

4. The **transform S3 bucket** contains the parquet files created by the transform lambda, timestamped and named according to the star schema tables. 

5. The **load lambda** (*Python and SQL*) loads the data into the data warehouse. It picks up the parquet files from the transform S3 bucket, and uses SQL to load the data into the Data Warehouse. 

6. The **data warehouse** (*SQL*) uses a star schema model to contain the transformed data. A change to the source database will be reflected in the data warehouse within 30 minutes.

7. **Data visualisation** - to be updated 

### Additional elements 

* **Terraform**: Terraform is used to manage the S3 buckets, lambdas, cloudwatch and associated permissions. 

* **CI/CD**: A CI/CD pipeline is set up to automatically run security and unit tests and execute the terraform actions on code that is pushed to github. 

* **Monitoring**: Cloudwatch logs are set up to monitor the end-to-end process

* **Alerting**: An error will trigger an email notification that specifies which lambda caused the error (if relevant)

* **Testing**: All Python code is tested, PEP8 compliant, and tested for security vulnerabilities.

### Data

The tables ingested from the operational database `totesys` are:
* counterparty
* currency
* department
* design
* staff
* sales_order
* address
* payment
* purchase_order
* payment_type
* transaction

The tables in the data warehouse are:

* fact_sales_order
* fact_purchase_orders
* fact_payment
* dim_transaction
* dim_staff
* dim_payment_type
* dim_location
* dim_design
* dim_date
* dim_currency
* dim_counterparty


## Contributors 

<!-- Should we have this section with our github usernames? -->



<!-- SOME SECTIONS OF OLD README KEPT FOR REFERENCE, TO BE DELETED WHEN NOT NEEDED -->

## The Data

The primary data source for the project is a moderately complex (but not very large) database called `totesys` which is meant to simulate the back-end data of a commercial application. Data is inserted and updated into this database several times a day. (The data itself is entirely fake and meaningless, as a brief inspection will confirm.)

Each project team will be given read-only access credentials to this database. The full ERD for the database is detailed [here](https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92).

In addition, you will be given credentials for a data warehouse hosted in the Northcoders AWS account. The data will have to be remodelled for this warehouse into three overlapping star schemas. You can find the ERDs for these star schemas:
 - ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
 - ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
 - ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca).

The tables to be ingested from `totesys` are:
|tablename|
|----------|
|counterparty|
|currency|
|department|
|design|
|staff|
|sales_order|
|address|
|payment|
|purchase_order|
|payment_type|
|transaction|

The list of tables in the complete warehouse is:
|tablename|
|---------|
|fact_sales_order|
|fact_purchase_orders|
|fact_payment|
|dim_transaction|
|dim_staff|
|dim_payment_type|
|dim_location|
|dim_design|
|dim_date|
|dim_currency|
|dim_counterparty|

However, for your minimum viable product, you need only populate the following:
|tablename|
|---------|
|fact_sales_order|
|dim_staff|
|dim_location|
|dim_design|
|dim_date|
|dim_currency|
|dim_counterparty|

This should be sufficient for a single [star-schema](https://dbdiagram.io/d/637a423fc9abfc611173f637).


## Visualisation
To demonstrate the use of the warehouse, you will be required to display some of the data in an application
that can read data in real-time from the warehouse. Examples of such applications could be:
- a BI dashboard, such as [AWS Quicksight](https://aws.amazon.com/quicksight/). Alternatives include the
  free tiers of well-known tools such as [Power BI](https://www.microsoft.com/en-gb/power-platform/products/power-bi)
  or [Tableau](https://www.tableau.com/en-gb). There is also the open-source [Superset](https://superset.apache.org/)
  tool. _Northcoders tutors can help you with the setup and configuration of Quicksight but if you choose to
  use any other tool, you must take responsibility for setting it up yourself._ 
- a Jupyter notebook containing graphical elements from a library such as [matplotlib](https://matplotlib.org/)
  or [Seaborn](https://seaborn.pydata.org/)
- a [Shiny app](https://shiny.posit.co/) or [Steamlit](https://streamlit.io/) front-end.

This aspect of the project should not be tackled until the final week of the course, more details will be given then. The major focus of your efforts should be to get the data into the data warehouse.

![img](./mvp.png)


### Required Components

You need to create:
1. A job scheduler or orchestration process to run the ingestion job and subsequent processes. You can
do this with AWS Eventbridge or with a combination of Eventbridge and AWS Step Functions. Since data has to be visible in the data warehouse within 30 minutes of being written to the database, you need to schedule your job to check for changes frequently.
1. An S3 bucket that will act as a "landing zone" for ingested data.
1. A Python application to check for changes to the database tables and ingest any new or updated data. It is **strongly** recommended that you use AWS Lambda as your computing solution. It is possible to use other computing tools, but it will probably be _much_ harder to orchestrate, monitor and deploy. The data should be saved in the "ingestion" S3 bucket in a suitable format. Status and error messages should be logged to Cloudwatch.
1. A Cloudwatch alert should be generated in the event of a major error - this should be sent to email.
1. A second S3 bucket for "processed" data.
1. A Python application to transform data landing in the "ingestion" S3 bucket and place the results in the "processed" S3 bucket. The data should be transformed to conform to the warehouse schema (see above). The job should be triggered by either an S3 event triggered when data lands in the ingestion bucket, or on a schedule. Again, status and errors should be logged to Cloudwatch, and an alert triggered if a serious error occurs.
1. A Python application that will periodically schedule an update of the data warehouse from the data in S3. Again, status and errors should be logged to Cloudwatch, and an alert triggered if a serious error occurs.
1. **In the final week of the course**, you should be asked to create a simple visualisation such as
  described above. In practice, this will mean creating SQL queries to answer common business questions. Depending on the complexity of your visualisation tool, other coding may be required too.