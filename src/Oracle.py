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
from config import config

def main():
    print (f"""Getting average yearly prices per region for all""")
    # read data through jdbc from Oracle

    appName = config['common']['appName']
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
    tableName=config['OracleVariables']['sourceTable']
    fullyQualifiedTableName = config['OracleVariables']['dbschema']+'.'+tableName
    print("reading from Oracle table")
    house_df = s.loadTableFromOracleJDBC(spark,fullyQualifiedTableName)
    house_df.printSchema()
    house_df.show(5,False)
    print(f"""\nAnnual House prices per regions in GBP""")
    # Workout yearly aversge prices
    df2 = house_df. \
                    select( \
                          F.date_format('datetaken','yyyy').cast("Integer").alias('YEAR') \
                        , 'REGIONNAME' \
                        , round(F.avg('averageprice').over(wSpecY)).alias('AVGPRICEPERYEAR') \
                        , round(F.avg('flatprice').over(wSpecY)).alias('AVGFLATPRICEPERYEAR') \
                        , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTERRACEDPRICEPERYEAR') \
                        , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSDPRICEPRICEPERYEAR') \
                        , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDETACHEDPRICEPERYEAR')). \
                    distinct().orderBy('datetaken', asending=True)
    df2.printSchema()
    df2.show(20,False)
    # write to Oracle table, all uppercase not mixed case and column names <= 30 characters in version 12.1
    s.writeTableToOracle(df2,"overwrite",config['OracleVariables']['dbschema'],config['OracleVariables']['yearlyAveragePricesAllTable'])
    print(f"""created {config['OracleVariables']['yearlyAveragePricesAllTable']}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
