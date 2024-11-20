[![CI](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/atreya-tadepalli-miniproject10/actions/workflows/cicd.yml)
### Mini Project 10 - Pyspark Usage

## Introduction

In this project, I worked on harnessing PySpark to execute SQL operations. In this project, I reviewed records associated with health failure, including blood pressure status, creatinine levels, and smoking habits, to better understand biological indicators of heart failure, segmented by age.

## Steps

1. The first step involved downloading pyspark and java appropriately within Codespaces. Once these were downloaded, the proper packages could be imported for use, such as SparkSession and requests.
   
2. The second step was to revise the methods originally used to perform SQL operations on Databricks to utilize Pyspark. I was able to revise methods such as extract and transform_load to utilize PySpark. Specifically with transform_load, I have specified the schema to upload the data into a dataframe.

3. Finally, I have prepared new methods to query the data and transform the data by adding new columns. These are ultimately executed in the main file, after creating a temporary view of the dataframe.

   a. `Query` - This method involved gathering the average values for health metrics for patients of each age. Specifically, I sought to understand the average creatinine_phosphokinase, serum_creatinine, serum_sodium, and age values, for each age value represented within the dataset. Additionally, I sought to gather these average values for females within the dataset, so I specified that in the SQL command below.

'SELECT AVG(creatinine_phosphokinase), AVG(serum_creatinine), AVG(serum_sodium), AVG(age)
FROM table_query
WHERE sex=1
GROUP BY age
ORDER BY age DESC;'  




   b. `Add_Column' - This method involved adding a new column to the dataset, conditional on another column. In this case, I performed a filter and set up a new variable to indicate if a patient had a low platelet count, or under 150,000 platelets (1), or if they exceeded that threshold. The below image shows the addition of one more column, `low_platelet_count`, within the dataset.

   <img width="895" alt="SQL-query-add_column" src="https://github.com/user-attachments/assets/81fa99ea-ecd5-4df4-bfc7-fd8bf0caa614">


## Results

The below image displays the results of the SQL operation. Additionally, this is also shared within the markdown file generated.

<img width="460" alt="SQL-query-pyspark" src="https://github.com/user-attachments/assets/7c64da88-1d8c-4d0e-be23-6c2e04edaa03">




