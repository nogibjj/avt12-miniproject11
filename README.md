[![CI](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml)
### Mini Project 10 - Pyspark Usage

## Introduction

In this project, I worked on harnessing PySpark to execute SQL operations. In this project, I reviewed records associated with health failure, including blood pressure status, creatinine levels, and smoking habits, to better understand biological indicators of heart failure, segmented by age.

## Steps

1. The first step involved downloading pyspark and java appropriately within Codespaces. Once these were downloaded, the proper packages could be imported for use, such as SparkSession and requests.
   
2. The second step was to revise the methods originally used to perform SQL operations on Databricks to utilize Pyspark. I was able to revise methods such as extract and transform_load to utilize PySpark. Specifically with transform_load, I have specified the schema to upload the data into a dataframe.

3. Finally, I have prepared new methods to query the data and transform the data by adding new columns. These are ultimately executed in the main file, after creating a temporary view of the dataframe.

   a. `Query` - This method involved gathering the average values for health metrics for patients of each age.



1. First thing to do on launch is to open a new shell and verify virtualenv is sourced.

Things included are:

* `Makefile`

* `Pytest`

* `pandas`

* `Ruff`:  

Run `make lint` which runs `ruff check`.  You can find out more info on [Ruff here](https://github.com/astral-sh/ruff).

* `Dockerfile`

* `GitHub copilot`

* `jupyter` and `ipython` 

* A base set of libraries for devops and web

* `githubactions`

## References

![1 1-function-essence-of-programming](https://github.com/nogibjj/python-ruff-template/assets/58792/f7f33cd3-cff5-4014-98ea-09b6a29c7557)



