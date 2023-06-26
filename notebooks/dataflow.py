# Databricks notebook source
# MAGIC %md
# MAGIC # Data pipeline example
# MAGIC #### Sample notebook that defines a dataflow using the delfner library

# COMMAND ----------

# DBTITLE 1,Import the class createDeltaTable
from com.marksandspencer.mands.main.Operations import createDeltaTable

df = spark.range(10)

createDeltaTable("df","dbfs:/tmp/sampletable", "default", "analyse_table_dlt")
# COMMAND ----------
# A sample of using the logger function
import logging
logger = logging.getLogger("demo logger")
logger.setLevel(logging.WARN)

# COMMAND ----------

logger.warning("this is a warn message")

