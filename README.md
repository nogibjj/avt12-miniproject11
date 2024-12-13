[![CI](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml)
### Mini Project 11 - Databricks ETL

## Introduction

In this project, I worked on preparing a datasink using Databricks, and ultimately accessing this data through a data pipeline to return the result of a query. The Databricks project is linked here:
https://dbc-c95fb6bf-a65d.cloud.databricks.com/browse/folders/1492636779888795?o=3670519680858392

## Data

The data utilized in this project comes from a publicly-available dataset on Github which concerns heart failure, and the characteristics associated with heart failures. It includes varaibles related to age, smoking status, sex, blood pressure, diabetes, and biological indicator amounts, such as creatinine phosphokinase.

## Steps

1. The first step involved connecting my Github repository to Databricks and preparing a new project. Following this, I developed three separate notebooks to mirror the previous week's assignment and work on extracting, transforming, and querying the dataset.
   
2. The second step was to extract the dataset and ultimately load it into a table in Databricks. TO do so, I created a schema under our Serverless State Warehouse, and directed a table to be created with the data provided within the dataset.

3. After establishing the table in Databricks, I sought to transform the table by creating a new table which included a new column to denote Senior status. If the age of a patient was over 65, I would mark as a senior in a new column. I would then select a subset of the columns to include in the new table, heart_transformed. Additionally, for this new table, I only retained rows for patients who had diabetes (diabetes==1).

4. Finally, I queried the data using the following SQL command: "SELECT * FROM ids706_data_engineering.at_heart_data.heart_failure_data_transformed WHERE smoking==1"

## Execution

To execute this series of steps, I used the Databricks Workflow function, and set up a workflow with extract, transform, and query in order. I include the diagram below, with successful notations following the completed ETL and querying process.

<img width="710" alt="image" src="https://github.com/user-attachments/assets/f947b9d9-a921-467f-ba1c-3eb3f81da6f4" />

## Results

Following the successful execution of the workflow, the query resulted in 30 records where the patient had diabetes and was a smoker. Of these patients, 40% died from the heart failure incident.

<img width="597" alt="image" src="https://github.com/user-attachments/assets/7bac9fbc-01c4-4e58-b24b-349cce431525" />


