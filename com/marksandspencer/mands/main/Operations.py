from pyspark.sql.utils import AnalysisException
from com.marksandspencer.mands.main.sparksession import spark
from pyspark.sql.functions import col


def tableExists(dbName, tblName):
    """
    desc:
        A static function for checking whether a given table exists.
        spark.catalog.tableExists method does not exist in python
        Although the scala method can be accessed using _jsparkSession, it is better to avoid using it

    args:
        dbName: String
        tblName: String

    return:
        Boolean - returns True or if it doesn't exists raises an exception

    example:
        tableExists("bronze_mp1", "vbak")

    tip:
        N/A
    """
    try:
        spark.read.table(dbName + "." + tblName)
        return True
    except AnalysisException:
        return False


def createDeltaTable(df, path, dbName, tblName, pCols=""):
    """
    desc:
        A static function for creating the target table using the given dataframe schema, database and table name

    args:
        df: DataFrame - A spark dataframe
        path: String - A path or an external location for the table
        dbName: String - Name of the database for creating the table
        tblName: String - Name of the table to be created
        pCols: String (Optional) - partition columns as a string with the column names separated with a comma

    return:
        N/A - Does not return anything. Just creates the table on the catalog

    example:
        df = spark.read.table("default.sample_table")
        createDeltaTable(df, "/user/tmp/table/vbak", "default", "sample_table_2")

    tip:
        1. This function will only create an external table
        2. The table will be created with the below table properties:
            delta.autoOptimize.optimizeWrite = true,
            delta.tuneFileSizesForRewrites = true,
            delta.dataSkippingNumIndexedCols = 10,
            delta.enableChangeDataCapture = true"
    """

    tblDDL = hiveDDL(df)

    partitions = f" \n PARTITIONED BY ({pCols})" if pCols else ""

    tblProps = "delta.autoOptimize.autoCompact = false, \n\
         delta.autoOptimize.optimizeWrite = true, \n\
         delta.tuneFileSizesForRewrites = true, \n\
         delta.dataSkippingNumIndexedCols = 10, \n\
         delta.enableChangeDataCapture = true"

    createTable = "CREATE TABLE IF NOT EXISTS {dbName}.{tblName} ({tblDDL}) \n USING DELTA {partitions} " \
                  "\n LOCATION \"{path}\" \n TBLPROPERTIES ({tblProps})".format(
                    dbName=dbName,
                    tblName=tblName,
                    tblDDL=tblDDL,
                    partitions=partitions,
                    path=path,
                    tblProps=tblProps
                    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbName}")

    spark.sql(createTable)
