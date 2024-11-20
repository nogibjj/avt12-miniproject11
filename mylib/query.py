from pyspark.sql.functions import col, when

def add_column(spark, heart_df):
    #Includes a transformation
    heart_df=heart_df.withColumn(
        "low_platelet_count",
        when(col("platelets") <= 150000, 1).otherwise(0))
    
    heart_df.createOrReplaceTempView("heart_platelet")
    return heart_df

def query(spark,heart_df,query, name = 'heart_platlet_1'):
    heart_df.createOrReplaceTempView(name)
    return spark.sql(query).show()

def describe(heart_df):
    heart_df.describe().toPandas().to_markdown()
    return heart_df.describe().show()