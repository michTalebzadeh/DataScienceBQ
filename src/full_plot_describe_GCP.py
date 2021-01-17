from __future__ import print_function
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
from matplotlib import pyplot as plt
from lmfit.models import LorentzianModel
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import sys

import config as v

def functionLorentzian(x, amp1, cen1, wid1, amp2,cen2,wid2, amp3,cen3,wid3):
    return (amp1*wid1**2/((x-cen1)**2+wid1**2)) +\
            (amp2*wid2**2/((x-cen2)**2+wid2**2)) +\
                (amp3*wid3**2/((x-cen3)**2+wid3**2))
def main():
    regionname = sys.argv[1]  ## parameter passed
    short = regionname.replace(" ", "").lower()
    print(f"""Getting plots for {regionname}""")
    appName = "ukhouseprices"
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    #
    # Get data from BigQuery table
    summaryTableName = v.fullyQualifiedoutputTableId
    start_date = "201001"
    end_date = "202001"
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    # Model predictions
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # read data from the Bigquery table summary
    print("\nreading data from "+v.fullyQualifiedoutputTableId)

    summary_df = spark.read. \
                  format("bigquery"). \
                  option("credentialsFile",v.jsonKeyFile). \
                  option("project", v.projectId). \
                  option("parentProject", v.projectId). \
                  option("dataset", v.targetDataset). \
                  option("table", v.targetTable). \
        load()
    df_10 = summary_df.filter(F.col("Date").between(f'{start_date}', f'{end_date}')). \
        select(F.date_format('Date',"yyyyMM").cast("Integer").alias("date"), 'flatprice', 'terracedprice', 'semidetachedprice', 'detachedprice')
    df_10.printSchema()
    print(df_10.toPandas().columns.tolist())
    p_dfm = df_10.toPandas()  # converting spark DF to Pandas DF
    # Non-Linear Least-Squares Minimization and Curve Fitting

    # Define model to be Lorentzian and deploy it
    model = LorentzianModel()
    n = len(p_dfm.columns)
    for i in range(n):
      if p_dfm.columns[i] != "date":   # yyyyMM is x axis in integer
         # it goes through the loop and plots individual average curves one by one and then prints a report for each y value
         vcolumn = p_dfm.columns[i]
         print(vcolumn)
         params = model.guess(p_dfm[vcolumn], x = p_dfm['date'])
         result = model.fit(p_dfm[vcolumn], params, x = p_dfm['date'])
         result.plot_fit()
         plt.margins(0.15)
         plt.subplots_adjust(bottom=0.25)
         plt.xticks(rotation=90)
         plt.xlabel("year/month", fontdict=v.font)
         plt.text(0.35,
                  0.45,
                  "Best-fit based on Non-Linear Lorentzian Model",
                  transform=plt.gca().transAxes,
                  color="grey",
                  fontsize=9
                  )
         plt.xlim(left=200900)
         plt.xlim(right=202100)
         if vcolumn == "flatprice": property = "Flat"
         if vcolumn == "terracedprice": property = "Terraced"
         if vcolumn == "semidetachedprice": property = "semi-detached"
         if vcolumn == "detachedprice": property = "detached"
         plt.ylabel(f"""{property} house prices in millions/GBP""", fontdict=v.font)
         plt.title(f"""Monthly {property} prices fluctuations in {regionname}""", fontdict=v.font)
         print(result.fit_report())
         plt.show()
         plt.close()

    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)


if __name__ == "__main__":
  print("\nworking on this code")
  main()
