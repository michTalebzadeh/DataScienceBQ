from pyspark.sql.functions import col

def filter_spark_data_frame(
    dataframe,
    column_name='age',
    value=20,
):
    return dataframe.where(col(column_name) > value)