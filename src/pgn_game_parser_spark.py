from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
import pandas as pd

spark = (
    SparkSession
    .builder
    .enableHiveSupport()
    .appName("SparkChessApp")
    .getOrCreate()
)

#get file path
path = 'C:/Users/phil_/Downloads/lichess_db_standard_rated_2013-01.pgn'

#read file into dataframe
df = spark.read.text(path)

#write to temp view


df_pvt = df.withColumn("RowLength",length("value"))
df_pvt.show(18,False)


