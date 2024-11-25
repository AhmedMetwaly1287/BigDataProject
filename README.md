![Extract](https://github.com/user-attachments/assets/e95a4645-15d7-4424-9c4a-3a763fb924c0)

# Airplane Crashes ETL Project

This project demonstrates an **end-to-end ETL (Extract, Transform, Load) pipeline** using the **Airplane Crashes Dataset**. It processes raw, erroneous data, ensures data quality, and stores it in a **MySQL database**. The cleaned data is then visualized with **Power BI**, featuring interactive dashboards enhanced with **tooltips**.

---

## Table of Contents
- [Overview](#overview)
- [Key Features](#key-features)
- [Technologies Used](#technologies)
- [Tasks](#tasks)
- [Installation](#installation)
- [Usage](#usage)
  - [Extract](#extract)
  - [Transform](#transform)
  - [Load](#load)
  - [Data Quality Checks](#data-quality-checks)
- [Power BI Dashboard](#power-bi-dashboard)
- [Disclaimer](#disclaimer)

---

## Overview

The **Airplane Crashes ETL Project** automates the workflow for ingesting, cleaning, and loading data. By leveraging modular Python functions, global configurations, and data quality checks, the pipeline prioritzes functions for code reusability and maintainability, leverages logging in order for easier debugging. The processed data supports insights into aviation safety trends via **interactive dashboards** in Power BI.

---

## Key Features

1. **Reusable Functions**: Each stage of the ETL pipeline is implemented as a function, making the code modular and easy to maintain.
2. **Global SparkSession**: Optimized performance by reusing a single SparkSession instance throughout the pipeline so that it isn't re-initialized everytime one of the functions are ran.
3. **Data Quality Assurance**: Pre- and post-transformation validation using **Great Expectations** to measure the effectiveness of our processing.
4. **Interactive Visualizations**: Power BI dashboards with tooltips provide deeper insights into trends.
5. **Error Handling**: Comprehensive logging and exception handling mechanisms.

---

## Technologies

- **Apache Spark (PySpark)**: For data processing and transformations.
- **MySQL**: As the destination for processed data.
- **Great Expectations**: For creating and running data quality checks.
- **Power BI**: To build interactive dashboards.
- **Python Logging**: For detailed logging of ETL operations.

---

## Tasks

1. **Extract**: Ingest data from flat files (e.g., CSV, JSON).
2. **Transform**: Clean and enrich the data (e.g., handling nulls, casting columns, adding IDs).
3. **Load**: Save the processed data into MySQL.
4. **Data Quality Checks**: Validate data before and after transformations.
5. **Visualize**: Create dynamic Power BI dashboards.

---

## Installation

### Prerequisites
- Python 3.11 or later
- MySQL Server
- Power BI Desktop

### Steps
1. Clone the repository:
```bash
git clone https://github.com/AhmedMetwaly1287/AirplaneCrashes.git
cd AirplaneCrashes
```
2. Install Python dependencies:
```bash
pip install pyspark great_expectations pymysql
```
- NOTE: Ensure your Apache Spark and PySpark versions match (in my case, it was version 3.5.0)

3. Set up MySQL and create a database:
```sql
CREATE DATABASE AirplaneCrashes;
```
- NOTE: Ensure you have the MySQL Connector JAR file installed in the **jars** folder in the directory where Spark is installed.
  
4. Configure great_expectations for data quality checks
```python
import great_expectations as gx

gx.get_context()
```

## Usage
### Extract
The Extract function loads data from flat files into a Spark DataFrame.

```python
from Extract import Extract

df = Extract(
    filepath="Data/raw/dataset.csv", 
    format="csv", 
    options={"header": "true", "inferSchema": "true"}
)
```
### Transform
The Transform function performs cleaning and enrichment operations, tailored to the dataset.
```
python
from Transform import Transform

transformed_df = Transform(df)
```

### Load
The Load function writes the processed data into a MySQL table.
```
python
from Load import Load

Load(
    df=transformed_df, 
    Database="AirplaneCrashes", 
    Table="ProcessedData", 
    mode="append"
)
```
### Data Quality Checks
Leverage Great Expectations to validate data integrity before and after transformations.

To run the suite please refer to the notebook where the same suite is ran for the raw and the transformed dataframe.

## Power BI Dashboard
The Power BI dashboard offers:

- Time Trends: Visualize crashes over the decades.
- Operator Analysis: Focused insights on airlines and operators.
- Detailed Tooltips: Hover over any chart for deeper insights. 

![image](https://github.com/user-attachments/assets/69580c5f-6c5c-4c4e-9803-81cbaf1f582e)

## Disclaimer

This project reflects my improvements, it was initially created as a task for my **Big Data Technologies (IS 365)** university course, it has gone through a lot of improvements and will go through a lot of improvements, it is my first ever project using Big Data/Data Tools so your feedback is highly appreciated and so is your participation!

