from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import matplotlib.pyplot as plt

import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
from config import config

def main():
    print (f"""Getting average yearly prices per region for all""")
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    print (f"""Getting Yearly percentages tables for {regionname}""")
    appName = "ukhouseprices"
    spark = s.spark_session(appName)
    # Get data from BigQuery table
    tableName = f"""{config['GCPVariables']['percentYearlyHousePriceChange']}_{short}"""
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    spark = s.setSparkConfBQ(spark)
    print("\nStarted at");uf.println(lst)
    read_df = s.loadTableintoBQ(spark, config['GCPVariables']['targetDataset'], tableName)
    summary_df = read_df.select(col("Year"), col("percent_change").alias("PercentYearlyChange"))
    p_df = summary_df.toPandas()
    print(p_df)
    p_df.plot(kind='bar', stacked=False, x='Year', y=['PercentYearlyChange'])
    plt.xlabel("Year", fontdict=config['plot_fonts']['font'])
    plt.ylabel("Annual Percent Property Price change", fontdict=config['plot_fonts']['font'])
    plt.title(f"""Property price fluctuations in {regionname} for the past 10 years """, fontdict=config['plot_fonts']['font'])
    plt.margins(0.15)
    plt.subplots_adjust(bottom=0.25)
    plt.show()
    plt.close()
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");
    uf.println(lst)

if __name__ == "__main__":
  print("\nworking on this code")
  main()
