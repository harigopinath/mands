# test createDeltaTable

from com.marksandspencer.mands.main.Operations import createDeltaTable
from com.marksandspencer.mands.main.sparksession import spark
from com.marksandspencer.mands.main.Utils import dbutils

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

def createDeltaTable_test():
    data_cdt = [(1, "chandler", "sarcastic"),
                (2, "joey", "innocent")
                ]

    schema_cdt = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("character", StringType(), True)
    ])

    df_cdt = spark.createDataFrame(data=data_cdt, schema=schema_cdt)

    createDeltaTable(df_cdt, "/tmp/streaming_testcase_createdeltatable", "default", "streaming_testcase_createdeltatable")

    assert spark.read.table("default.streaming_testcase_createdeltatable").schema == StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("character", StringType(), True)])

    spark.sql("drop table if exists default.streaming_testcase_createdeltatable")

    dbutils.fs.rm("/tmp/streaming_testcase_createdeltatable", recurse=True)
