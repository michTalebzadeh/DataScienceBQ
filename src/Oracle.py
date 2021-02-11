from __future__ import print_function
import sys
from src.config import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')

class Oracle():
    def __init__(self, spark_session):
        self.spark = spark_session
        self.config = config

    def extractOracleData(self):
        # read data through jdbc from Oracle
        tableName = self.config['OracleVariables']['sourceTable']
        fullyQualifiedTableName = self.config['OracleVariables']['dbschema']+'.'+tableName
        user = self.config['OracleVariables']['oracle_user']
        password = self.config['OracleVariables']['oracle_password']
        driver = self.config['OracleVariables']['oracle_driver']
        fetchsize = self.config['OracleVariables']['fetchsize']
        print("reading from Oracle table")
        house_df = s.loadTableFromJDBC(self.spark,oracle_url,fullyQualifiedTableName,user,password,driver,fetchsize)
        house_df.printSchema()
        house_df.show(5,False)
        return house_df

    def transformOracleData(self, house_df):

        print(f"""\nAnnual House prices per regions in GBP""")
        # Workout yearly average prices
        wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
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
        return df2

    def loadIntoOracleTable(self, df2):
        # write to Oracle table, all uppercase not mixed case and column names <= 30 characters in version 12.1
        tableName = self.config['OracleVariables']['yearlyAveragePricesAllTable']
        fullyQualifiedTableName = self.config['OracleVariables']['dbschema']+'.'+tableName
        user = self.config['OracleVariables']['oracle_user']
        password = self.config['OracleVariables']['oracle_password']
        driver = self.config['OracleVariables']['oracle_driver']
        mode = self.config['OracleVariables']['mode']
        s.writeTableWithJDBC(df2,oracle_url,fullyQualifiedTableName,user,password,driver,mode)
        print(f"""created {config['OracleVariables']['yearlyAveragePricesAllTable']}""")
        # read data to ensure all loaded OK
        fetchsize = self.config['OracleVariables']['fetchsize']
        read_df = s.loadTableFromJDBC(self.spark,oracle_url,fullyQualifiedTableName,user,password,driver,fetchsize)
        # check that all rows are there
        if df2.subtract(read_df).count() == 0:
            print("Data has been loaded OK to Oracle table")
        else:
            print("Data could not be loaded to Oracle table, quitting")
            sys.exit(1)

if __name__ == "__main__":
    appName = config['common']['appName']
    spark_session = s.spark_session(appName)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    oracle = Oracle(spark_session)
    house_df = oracle.extractOracleData()
    df2 = oracle.transformOracleData(house_df)
    oracle.loadIntoOracleTable(df2)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)




