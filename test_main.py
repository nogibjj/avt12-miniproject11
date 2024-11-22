"""
Test goes here

"""

from pyspark.sql import SparkSession
from mylib.extract import extract_load
import os
from mylib.transform_load import transform_table
from mylib.query import query



spark = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()


def test_extract_load():

    output_path = extract_load(url="https://shorturl.at/5YexG", 
    file_path="data/heart-failure.csv",
    directory="data")
    assert os.path.exists(output_path)

def test_transform():
    df = transform_table("heart_failure_load","heart-failure-transform")
    assert df is None, "Dataframe is None; load_data failed."

def test_query():
    extract_load(url="https://shorturl.at/5YexG", 
    file_path="data/heart-failure.csv",
    directory="data")
    transform_table("heart-failure-load","heart-failure-transform")
    assert query("SELECT AVG(age) FROM heart-failure-transform WHERE smoking=1") is None
    