from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd


spark = (SparkSession
         .builder
         .getOrCreate()
         )

df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ("time", "id", "v1"))
df2 = spark.createDataFrame(
    [(20000101, 1, "x"), (20000101, 2, "y")],
    ("time", "id", "v2"))
def asof_join(l, r):
    return pd.merge_asof(l, r, on="time", by="id")
df2 = df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(asof_join, "time int, id int, v1 double, v2 string")

df2.coalesce(1).write.parquet("dbfs:/home/riebeeck.vanniekerk@databricks.com/databricks_connect_test/pyspark_pandastest.parquet")