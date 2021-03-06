# Data Warehouse on AWS with Redshift

The purpose of this project is to build an adapted data model thanks to python to load data in a S3 bucket and wrangle them into a star schema (see the ERD).

### Prerequisite

1. Install [Python 3.x](https://www.python.org/).

2. This project is build with **conda** instead of **pip**.
Install [anaconda](https://www.anaconda.com/distribution/) or modify the script to make use of pip.

3. You need also to have a **AWS Redshift cluster** up and running (4 to 8 nodes suggested)

### Main Goal
The compagny Sparkify need to analyses theirs data to better know the way users (free/paid) use theirs services.
With this data model we will be able to ask question like When? Who? Where? and What? about the data.
The task is to build an ETL Pipeline that extract data from a S3, stagging it in Redshift to be able to transform the data into a Star Schema (Dimensional and Fact Tables) to let the Analytics Team to find insights easily. 

### Data Model
![Song ERD](./Song_ERD.png)

This data model is called a **start schema** data model. At it's aim is a **Fact Table -songplays-** that containg fact on song play like user agent, location, session or user's level and then have columns of **foreign keys (FK)** of **4 dimension tables** :

* **Songs** table with data about songs
* **Artists** table
* **Users** table
* **Time** table

This model enable search with the minimum **SQL JOIN** possible and enable fast read queries.

### Run it
Few steps

1. Launch **create_tables.py** to prepare the database
2. Run **etl.py** to wrangle the data