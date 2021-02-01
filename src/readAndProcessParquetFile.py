from __future__ import print_function
from src.config import config
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')


def main():
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    spark = s.setSparkConfBQ(spark)
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    print(f"""Reading from parquet file {config['ParquetVariables']['sourceSmall']}""")
    # read from the source file
    currentSnapshot = spark.read.load(config['ParquetVariables']['sourceSmall'])
    currentSnapshot.printSchema()
    #currentSnapshot.show()
    print(f"""\nRows in source file is""", currentSnapshot.count())
    print(currentSnapshot.rdd.getStorageLevel())
    currentSnapshot = currentSnapshot.repartition(5)
    print(currentSnapshot.rdd.getStorageLevel())
    # read from delta files
    deltaFile = "gs://etcbucket/randomdata/staging/randomdatapy_208150201_208150210"
    newAddedDeltaFiles = spark.read.load(deltaFile)
    # check missing records with source file
    # find out IDs that do not exist in source
    newAddedDeltaFiles.createOrReplaceTempView("newAddedDeltaFiles")
    currentSnapshot.createOrReplaceTempView("currentSnapshot")
    sqltext = """SELECT
                     newAddedDeltaFiles.ID
                   , newAddedDeltaFiles.CLUSTERED
                   , newAddedDeltaFiles.SCATTERED
                   , newAddedDeltaFiles.RANDOMISED
                   , newAddedDeltaFiles.RANDOM_STRING
                   , newAddedDeltaFiles.SMALL_VC
                   , newAddedDeltaFiles.PADDING 
                 FROM newAddedDeltaFiles 
                 LEFT OUTER JOIN currentSnapshot ON newAddedDeltaFiles.ID = currentSnapshot.ID
                 WHERE currentSnapshot.ID IS NULL ORDER BY newAddedDeltaFiles.ID"""
    print(f"""\nRows in deltafiles that do not exist in source file""", currentSnapshot.count())
    missingRows = spark.sql(sqltext)
    newSnapshot = currentSnapshot.union(missingRows)
    print(newSnapshot.orderBy(col("ID")).show(10000))
    sys.exit()
    #spark.sql(sqltext).write.mode(saveMode)
    print(f"""Writing to parquet file {config['ParquetVariables']['targetLocation']}""")
    df2.write.mode(config['ParquetVariables']['overwrite']).parquet(config['ParquetVariables']['targetLocation'])
    df3 = spark.read.load(config['ParquetVariables']['targetLocation'])
    print(f"""Reading from parquet file {config['ParquetVariables']['targetLocation']}""")
    print(f"""\nRows in target table is""", df3.count())
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
