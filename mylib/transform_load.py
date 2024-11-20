from pyspark.sql import SparkSession
from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
     FloatType 
)

PATH = 'data/heart-failure.csv'

#load the csv file and insert into a new databricks database
def trans_load(spark, path = PATH):
    '''Takes the csv file and loads at as a Spark df'''
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
    
    df = spark.read.option("header", "true").schema(schema).csv(path)

    return df

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Heart_Failure").getOrCreate()
    output = trans_load(spark)
    print('Number of columns:', len(output.columns))
    spark.stop()