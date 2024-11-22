from pyspark.sql import SparkSession
from pyspark.sql.functions import when


def transform_table(source_name,transformed_name):
    spark=SparkSession.builder.appName("transform-table").getOrCreate()
    source_df=spark.read.table(source_name)
    source_df=source_df.withColumn("senior_citizen",
                                   when(source_df["age"]>65,1)
                                   .otherwise(0))
    source_df.write.format("delta").mode("append").saveAsTable(transformed_name)
    print(f'Transformed data write into >{transformed_name}')

    return source_df.show()

if __name__ == '__main__':
    transform_table("heart-failure-load","heart-failure-transform")