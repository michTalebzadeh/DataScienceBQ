from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
from src.config import config

def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    print(f"""Creating Yearly percentage tables for {regionname}""")
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    #    # Get data from BigQuery table
    tableName="yearlyaveragepricesAllTable"
    start_date = "2010"
    end_date = "2020"
    yearTable = f"""{config['GCPVariables']['percentYearlyHousePriceChange']}_{short}"""
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    spark = s.setSparkConfBQ(spark)
    read_df = s.loadTableintoBQ(spark,config['GCPVariables']['targetDataset'],config['GCPVariables']['yearlyAveragePricesAllTable'])
    house_df = read_df.filter((col("Year").between(f'{start_date}', f'{end_date}')) & (lower(col("regionname"))== f'{regionname}'.lower()))
    wSpecPY = Window().orderBy('regionname', 'Year')
    df_lagY = house_df.withColumn("prev_year_value", F.lag(house_df['AVGPricePerYear']).over(wSpecPY))
    resultY = df_lagY.withColumn('percent_change', F.when(F.isnull(house_df.AVGPricePerYear - df_lagY.prev_year_value), 0). \
                             otherwise(F.round(((house_df.AVGPricePerYear - df_lagY.prev_year_value) * 100.) / df_lagY.prev_year_value, 1)))
    print(f"""\nYear House price changes in {regionname} in GBP""")
    rsY = resultY.select('Year', 'AVGPricePerYear', 'prev_year_value', 'percent_change')
    rsY.show(36, False)
    s.writeTableToBQ(rsY, "overwrite", config['GCPVariables']['targetDataset'],yearTable)
    print(f"""Created {yearTable}""")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
