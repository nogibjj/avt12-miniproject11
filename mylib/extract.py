from pyspark.sql import SparkSession 
from pyspark.sql.types import (
    StructType, 
    StructField, 
    IntegerType, 
    FloatType 
)
import os
import requests



def extract_load(url="https://shorturl.at/5YexG", 
    file_path="data/heart_failure.csv",
    directory="data"):

    if os.path.dirname(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with requests.get(url) as r:
        with open(file_path, 'wb') as f:
            f.write(r.content)

    spark = SparkSession.builder \
    .appName("Extract_Load") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
    #hf_df=pd.read_csv(url)
    #print(hf_df.head())
    #heart_failure_df=spark.createDataFrame(hf_df)
    schema = StructType([
                StructField("age", IntegerType(), True),
                StructField("anaemia", IntegerType(), True),
                StructField("creatinine_phosphokinase", IntegerType(), True),
                StructField("diabetes", IntegerType(), True),
                StructField("ejection_fraction", IntegerType(), True),
                StructField("high_blood_pressure", IntegerType(), True),
                StructField("platelets", IntegerType(), True),
                StructField("serum_creatinine", FloatType(), True),
                StructField("serum_sodium", IntegerType(), True),
                StructField("sex", IntegerType(), True),
                StructField("smoking", IntegerType(), True),
                StructField("time", IntegerType(), True),
                StructField("DEATH_EVENT", IntegerType(), True)
            ])
    
    heart_failure_df = spark.read.option("header", "true").schema(schema).csv(file_path)
    heart_failure_df.write.format("delta").mode("append").saveAsTable("heart_failure_load")
    print("Successfully extracted and loads data")
    return file_path

if __name__ == '__main__':
    extract_load()