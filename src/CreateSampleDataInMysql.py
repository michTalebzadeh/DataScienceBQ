from __future__ import print_function
import pytest
import sys
from src.config import config
from src.config import ctest, test_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from sparkutils import sparkstuff as s
from pyspark.sql.window import Window
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')

@pytest.fixture(scope = "session")
def extractHiveData():
    print(f"""Getting average yearly prices per region for all""")
    # read data through jdbc from Hive
    spark_session = s.spark_session(ctest['common']['appName'])
    tableName = config['GCPVariables']['sourceTable']
    fullyQualifiedTableName = config['hiveVariables']['DSDB'] + '.' + tableName
    print("reading from Hive table")
    house_df = s.loadTableFromHiveJDBC(spark_session, fullyQualifiedTableName)
    # sample data equally n rows from Kensington and Chelsea and n rows from City of Westminster
    num_rows = int(config['MysqlVariables']['read_df_rows']/2)
    house_df = house_df.filter(col("regionname") == "Kensington and Chelsea").limit(num_rows).unionAll(house_df.filter(col("regionname") == "City of Westminster").limit(num_rows))
    #house_df.printSchema()
    #house_df.show(5, False)
    return house_df

@pytest.fixture(scope = "session")
def loadIntoMysqlTable(house_df):
    # write to Mysql table
    s.writeTableToMysql(house_df,"overwrite",config['MysqlVariables']['dbschema'],config['MysqlVariables']['sourceTable'])
    print(f"""created {config['MysqlVariables']['sourceTable']}""")

@pytest.fixture(scope = "session")
def readSourceData():
    # read source table
    table = ctest['statics']['dbschema'] + '.' + ctest['statics']['sourceTable']
    spark_session = s.spark_session(ctest['common']['appName'])
    # Read the test table
    try:
        read_df = spark_session.read. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable", table). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            load()
        return read_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

@pytest.fixture(scope = "session")
def transformData():
    # 2) extract
    read_df = readSourceData()
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"), 'regionname')
    try:
        transformation_df = read_df. \
            select( \
            F.date_format('datetaken', 'yyyy').cast("Integer").alias('YEAR') \
            , 'REGIONNAME' \
            , round(F.avg('averageprice').over(wSpecY)).alias('AVGPRICEPERYEAR') \
            , round(F.avg('flatprice').over(wSpecY)).alias('AVGFLATPRICEPERYEAR') \
            , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTERRACEDPRICEPERYEAR') \
            , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSDPRICEPRICEPERYEAR') \
            , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDETACHEDPRICEPERYEAR')). \
            distinct().orderBy('datetaken', asending=True)
        return transformation_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

@pytest.fixture(scope = "session")
def saveData():
    # Write to test target table
    transformation_df = transformData()
    try:
        transformation_df. \
            write. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable",
                   ctest['statics']['dbschema'] + '.' + ctest['statics']['yearlyAveragePricesAllTable']). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            mode(ctest['statics']['mode']). \
            save()
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

@pytest.fixture(scope = "session")
def readSavedData():
    # read target table to tally the result
    table = ctest['statics']['dbschema'] + '.' + ctest['statics']['yearlyAveragePricesAllTable']
    spark_session = s.spark_session(ctest['common']['appName'])
    try:
        readSavedData_df = spark_session.read. \
            format("jdbc"). \
            option("url", test_url). \
            option("driver", ctest['statics']['driver']). \
            option("dbtable", table). \
            option("user", ctest['statics']['user']). \
            option("password", ctest['statics']['password']). \
            load()
        return readSavedData_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)