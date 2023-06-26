from com.marksandspencer.mands.main.Utils import dbutils
import glob
from pyspark.sql.functions import raise_error


def pathExists(tablePath):
    """
    A Function for determining whether a table path already exists. The assumption is, if the table path doesn't exists, then this is the first run else it is not.
    """
    try:
        dbutils.fs.ls(tablePath)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise_error(f"Unknown exception encountered while validating the path {tablePath}")


def srcPathExists(srcPath):
    """
    A Function for determining whether a srcPath already exists.
    """
    # This glob
    fileList = glob.glob(srcPath)
    if not fileList:
        return False
    else:
        return True
