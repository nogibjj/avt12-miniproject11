"""
Main cli or app entry point
"""
from pyspark.sql import SparkSession
from mylib.extract import extract
from mylib.transform_load import trans_load
from mylib.query import query, describe, add_column

query12="""
SELECT MAX(creatinine_phosphokinase), AVG(ejection_fraction),\
      MEDIAN(high_blood_pressure), MEDIAN(anaemia)
FROM heart-failure-transform
WHERE (smoking=1 and platelets>100000)
GROUP BY age
ORDER BY age DESC;
"""


if __name__ == "__main__":
    extract()
    spark = SparkSession.builder.appName("heart-data").getOrCreate()
    result_table = trans_load(spark)
    describe(result_table)
    print(add_column(spark,result_table))
    result_table.show()
    result_table.createOrReplaceTempView("table_query")
    query(spark,result_table,query12)
    spark.stop()    




