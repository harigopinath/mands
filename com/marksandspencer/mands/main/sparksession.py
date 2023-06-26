from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import logging


spark = (SparkSession.builder
         .getOrCreate())

dbutils = DBUtils(spark)
