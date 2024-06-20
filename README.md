# Fetch Rewards Coding Exercise

## Relational Data Model

I created a relational data model using LucidChart that is in [ERD_Fetch](https://github.com/arvindkravi/fetch_analysis/blob/main/ERD_Fetch.pdf).

## SQL queries

I have tested and written all queries on MySQL Workbench which can be found in [SQL_Fetch](https://github.com/arvindkravi/fetch_analysis/blob/main/SQL_Fetch.pdf).

### Assumptions:

- I created a new bridge table between Receipts and Brands.
- While there are quality issues in the data provided, I have written these queries with the presumption that the data is clean and there are no anomalies.

## Data Quality

Data quality checks can be found in the [DataQuality_Fetch.py](https://github.com/arvindkravi/fetch_analysis/blob/main/DataQuality_Fetch.py) python file.

The potential issues I discovered were the following:

- Purchase Date had 448 missing values.
- There were 461 rows where purchase date is greater than date scanned.
- There were 551 rows where date scanned is after the date the receipt was processed.
- There is a record which has item purchase count as 689 which seems unlikely.
- According to the data, the column ‘role’ should only have ‘consumer’ but it also has ‘fetch-staff’.

## Email to Stakeholders

The [Email_Fetch](https://github.com/arvindkravi/fetch_analysis/blob/main/Email_Fetch.pdf) file contains the sample email addressing the details about the project.
