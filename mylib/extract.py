from pyspark.sql import SparkSession 
import os
import requests
import pandas as pd


def extract_load(url="https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/heart_failure_clinical_records_dataset.csv", 
    file_path="data/heart_failure.csv"):

    if os.path.dirname(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)

    spark = SparkSession.builder \
    .appName("Extract_Load") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",\
             "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
    hf_df=pd.read_csv(url)
    print(hf_df.head())
    heart_failure_df=spark.createDataFrame(hf_df)
    print(type(heart_failure_df))
    heart_failure_df.write.format("delta").mode("append").saveAsTable("heart_failure_load")
    print("Successfully extracted and loads data")
    return file_path

if __name__ == '__main__':
    extract_load()