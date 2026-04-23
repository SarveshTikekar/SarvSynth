# This will render a spark session for all the etl_pipeline and analytics pipeline 

from typing import final
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from contextlib import contextmanager

@contextmanager
def get_spark_session(app_name: str = "SarvSynth"):
    """
    Initializes a Spark session optimized for a 4-vCPU runner.
    Ensures the session is stopped correctly after the 'with' block.
    """
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

    try:
        yield spark_session
    finally:
        spark_session.stop()
