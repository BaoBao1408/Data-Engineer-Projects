# Project Overview

In this project, I created a data pipeline using Airflow.  The pipeline will download podcast episodes. I stored our results in a Postgres database that we can easily query.


**Project Steps**

* Download the podcast metadata xml and parse
* Create a Postgres database to hold podcast metadata

## Code

File overview:

* `podcast_summary.py` - the code to create a data pipeline
* `dockerfile` - Docker file for Airflow Image 
* `docker-compose.yml` - service container which we are use 

# Local Setup

## Installation

* Docker Dekstop
* Vscode
* Python

# Setup Instructions

## Step 1: Download the above three files

* `podcast_summary.py`
* `dockerfile`
* `docker-compose.yml`

## Step 2: Make Directory for Project

```bash
mkdir podcast
cd podcast
```

## Step 3: Put these three file into that Directory

## Step 4: Make Directory for dags, logs, plugin

```bash
mkdir dags logs plugin
```

## Step 5: Put the `podcast_summary.py` into dags

## Step 6: Build the Image

```bash
docker build -t airflow-podcast .
```

## Step 7: Start Airflow Container

```bash
docker-compose up -d
```

## Step 8: Access Airflow UI

```bash
http://localhost:8080

```

## Step 9: Make Postgres Connection

[Guide for Connection Making](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui)


## Step 10: Run the Dag

Locate the `podcast_summary` DAG in the Airflow UI and trigger it to start the podcast data extraction, transformation, and loading process.


# Contact

For questions or feedback, please create issue or contact baoquocnguyen1408@gmail.com