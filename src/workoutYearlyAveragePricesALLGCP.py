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
    print (f"""Getting average yearly prices per region for all""")
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    spark = s.setSparkConfBQ(spark)
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    wSpecY = Window().partitionBy(F.date_format('Date',"yyyy"), 'regionname')
    house_df = s.loadTableFromBQ(spark,config['GCPVariables']['sourceDataset'],config['GCPVariables']['sourceTable'])
    house_df.printSchema()
    house_df.show(2, False)

    print(f"""\nAnnual House prices per regions in GBP""")
    # Workout yearly aversge prices
    df2 = house_df. \
                    select( \
                          F.date_format('Date', 'yyyy').cast("Integer").alias('year') \
                        , 'regionname' \
                        , round(F.avg('averageprice').over(wSpecY)).alias('AVGPricePerYear') \
                        , round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
                        , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTerracedPricePerYear') \
                        , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
                        , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
                    distinct().orderBy('Date', asending=True)
    df2.show(20,False)
    s.writeTableToBQ(df2,"overwrite",config['GCPVariables']['targetDataset'],config['GCPVariables']['yearlyAveragePricesAllTable'])
    print(f"""created {config['GCPVariables']['yearlyAveragePricesAllTable']}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
