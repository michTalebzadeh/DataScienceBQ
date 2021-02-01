from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import matplotlib.pyplot as plt
from lmfit.models import LinearModel, LorentzianModel, VoigtModel
from pyspark.sql.functions import array, col

import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
from config import config

def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    appName = config['common']['appName']
    spark = s.spark_session(appName)
    spark = s.setSparkConfBQ(spark)
    # Get data from BigQuery table
    start_date = "201001"
    end_date = "202001"
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    # Model predictions
    read_df = s.loadTableFromBQ(spark,config['GCPVariables']['sourceDataset'],config['GCPVariables']['sourceTable'])
    df_10 = read_df.filter(F.date_format('Date',"yyyyMM").cast("Integer").between(f'{start_date}', f'{end_date}') & (lower(col("regionname"))== f'{regionname}'.lower())). \
            select(F.date_format('Date',"yyyyMM").cast("Integer").alias("Date") \
                 , round(col("flatprice")).alias("flatprice") \
                 , round(col("terracedprice")).alias("terracedprice")
                 , round(col("semidetachedprice")).alias("semidetachedprice")
                 , round(col("detachedprice").alias("detachedprice")))
    print(df_10.toPandas().columns.tolist())
    p_dfm = df_10.toPandas()  # converting spark DF to Pandas DF

    # Non-Linear Least-Squares Minimization and Curve Fitting
    # Define model to be Lorentzian and depoly it
    model = LorentzianModel()
    n = len(p_dfm.columns)
    for i in range(n):
      if (p_dfm.columns[i] != 'Date'):   # yyyyMM is x axis in integer
         # it goes through the loop and plots individual average curves one by one and then prints a report for each y value
         vcolumn = p_dfm.columns[i]
         print(vcolumn)
         params = model.guess(p_dfm[vcolumn], x = p_dfm['Date'])
         result = model.fit(p_dfm[vcolumn], params, x = p_dfm['Date'])
         # plot the data points, initial fit and the best fit
         plt.plot(p_dfm['Date'], p_dfm[vcolumn], 'bo', label = 'data')
         plt.plot(p_dfm['Date'], result.init_fit, 'k--', label='initial fit')
         plt.plot(p_dfm['Date'], result.best_fit, 'r-', label='best fit')
         plt.legend(loc='upper left')
         plt.xlabel("Year/Month", fontdict=config['plot_fonts']['font'])
         plt.text(0.35,
                  0.55,
                  "Fit Based on Non-Linear Lorentzian Model",
                  transform=plt.gca().transAxes,
                  color="grey",
                  fontsize=9
                  )
         if vcolumn == "flatprice": property = "Flat"
         if vcolumn == "terracedprice": property = "Terraced"
         if vcolumn == "semidetachedprice": property = "semi-detached"
         if vcolumn == "detachedprice": property = "detached"
         plt.ylabel(f"""{property} house prices in millions/GBP""", fontdict=config['plot_fonts']['font'])
         plt.title(f"""Monthly {property} price fluctuations in {regionname}""", fontdict=config['plot_fonts']['font'])
         plt.xlim(200901, 202101)
         print(result.fit_report())
         plt.show()
         plt.close()
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");
    uf.println(lst)

if __name__ == "__main__":
    print("\nworking on this code")
    main()