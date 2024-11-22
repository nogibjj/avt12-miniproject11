"""
Main cli or app entry point
"""
from pyspark.sql import SparkSession
from mylib.extract import extract_load
from mylib.transform_load import transform_table
from mylib.query import query

query12="""
SELECT MAX(creatinine_phosphokinase), AVG(ejection_fraction),\
      MEDIAN(high_blood_pressure), MEDIAN(anaemia)
FROM heart-failure-transform
WHERE (smoking=1 and platelets>100000)
GROUP BY age
ORDER BY age DESC;
"""


if __name__ == "__main__":
    extract_load()
    spark = SparkSession.builder.appName("heart-data").getOrCreate()
    result_table = transform_table(spark)
    #describe(result_table)
    query(query12) 




