"""
Main cli or app entry point
"""
from pyspark.sql import SparkSession
from mylib.extract import extract
from mylib.transform_load import trans_load
from mylib.query import query, describe, add_column

query12="""
SELECT AVG(creatinine_phosphokinase), AVG(serum_creatinine), AVG(serum_sodium), AVG(age)
FROM table_query
WHERE sex=1
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




