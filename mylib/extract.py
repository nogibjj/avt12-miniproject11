from pyspark.sql import SparkSession 
import pandas as pd



def extract_load(url="https://shorturl.at/5YexG", 
            file_path="data/heart-failure.csv"
):
    #Extract a url to a file path and load to Databricks
    spark = SparkSession.builder.appName("Extract_Load").getOrCreate()
    hf_df=pd.read_csv(url)
    print(hf_df.head())
    heart_failure_df=spark.createDataFrame(hf_df)
    heart_failure_df.write.format("delta").mode("append").saveAsTable("heart_failure_load")
    print("Successfully extracted and loads data")
    return file_path

if __name__ == '__main__':
    extract_load()