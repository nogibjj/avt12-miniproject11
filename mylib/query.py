from pyspark.sql.functions import col, when
import pandas as pd
from pyspark.sql import SparkSession
"""
def add_column(spark, heart_df):
    #Includes a transformation
    heart_df=heart_df.withColumn(
        "low_platelet_count",
        when(col("platelets") <= 150000, 1).otherwise(0))
   
    heart_df.createOrReplaceTempView("heart_platelet")
    return heart_df
""" 
def query(query):
    spark=SparkSession.builder.appName("heart-failure-transform").getOrCreate()
    return spark.sql(query).show()

#def describe(heart_df):
    #heart_df.describe().toPandas().to_markdown()
    #return heart_df.describe().show()

