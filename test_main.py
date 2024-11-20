"""
Test goes here

"""

from pyspark.sql import SparkSession
from mylib.extract import extract
import os
from mylib.transform_load import trans_load
from mylib.query import query

spark = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()


def test_extract_csv():

    output_path = extract(url="https://shorturl.at/5YexG", 
            file_path="data/heart-failure.csv",
            directory="data",
)

    assert os.path.exists(output_path)

def test_load():
    df = trans_load(spark, path = 'data/heart-failure.csv')
    assert df is not None, "Dataframe is None; load_data failed."
    assert df.count() > 0, "Dataframe is empty; load_data did not load data correctly."

def test_query():
    result_table = trans_load(spark)
    result_table.createOrReplaceTempView("test_query")
    test_query12="SELECT AVG(age) FROM test_query WHERE smoking=1"
    assert query(spark,result_table,test_query12) is None