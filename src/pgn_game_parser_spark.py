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
df.show(19,False)

#write to temp view
df.createOrReplaceTempView("games")
#spark.sql("SELECT COUNT(*) FROM games").show()

#create column to pivot on
#uses SQL expressions, not python
df_row_type = df.withColumn("RowValue",expr("CASE"+ 
                                            " WHEN left(value,6) = '[Event' THEN 'Event'"+ 
                                            " WHEN left(value,5) = '[Site' THEN 'GameLink'"+
                                            " WHEN left(value,16) = '[WhiteRatingDiff' THEN 'WhiteRatingChange'"+
                                            " WHEN left(value,16) = '[BlackRatingDiff' THEN 'BlackRatingChange'"+
                                            " WHEN left(value,9) = '[WhiteElo' THEN 'WhiteELO'"+ 
                                            " WHEN left(value,9) = '[BlackElo' THEN 'BlackELO'"+ 
                                            " WHEN left(value,7) = '[White ' THEN 'WhitePlayer'"+ 
                                            " WHEN left(value,7) = '[Black ' THEN 'BlackPlayer'"+ 
                                            " WHEN left(value,7) = '[Result' THEN 'Result'"+ 
                                            " WHEN left(value,8) = '[UTCDate' THEN 'Date'"+ 
                                            " WHEN left(value,8) = '[UTCTime' THEN 'Time'"+
                                            " WHEN left(value,8) = '[Opening' THEN 'Opening'"+
                                            " WHEN left(value,12) = '[TimeControl' THEN 'TimeControl'"+
                                            " WHEN left(value,12) = '[Termination' THEN 'GameTermination'"+
                                            " WHEN left(value,2) = '1.' THEN 'GameMoves'"+
                                            " WHEN left(value,4) = '[ECO' THEN 'ECO'"+
                                            " WHEN left(value,11) = '[WhiteTitle' THEN 'WhitePlayerTitle'"+
                                            " WHEN left(value,11) = '[BlackTitle' THEN 'BlackPlayerTitle'"+
                                            " WHEN value = '' THEN 'EmptyRow'"+
                                            " ELSE '???'"+
                                            " END"))
df_row_type.show(19,False)

#create "identity column that will be used for min/max row values for each game"
#also filter out empty rows
#not sure if I can rely on monotonically_increasing_id() to be continuous (without gaps) but will assume I can for now
df_id = df_row_type.where("RowValue != 'EmptyRow'").withColumn("ID",monotonically_increasing_id())
df_id.show(19,False)

#load 1st iteration into table
df_id.createOrReplaceTempView("games_with_overall_id")
spark.sql("SELECT COUNT(*) FROM games_with_overall_id").show()

spark.sql("""SELECT RowValue,COUNT(*) FROM games_with_overall_id GROUP BY RowValue""").show(36)
spark.sql("""SELECT * FROM games_with_overall_id WHERE RowValue = '???'""").show(36)







#row number on the data to line up each to set up a pivot later
df_row_num = spark.sql("SELECT *,row_number() OVER (PARTITION BY RowValue ORDER BY NULL)totalrownum FROM games")
df_row_num.show(36,truncate=False)

#replace temp table with newly added column
df_row_num.createOrReplaceTempView("games")
spark.sql("SELECT * FROM games WHERE RowValue = ").show(36)

#pivot the data on "occurance (rownum partitioned by Rowvalue calculated column)"

df_pvt_single_game= df_pvt_single_game.groupBy("rownum").pivot("RowValue",["Event","GameLink","WhiteRatingChange","BlackRatingChange","WhiteELO","BlackELO","WhitePlayer","BlackPlayer","Result","Date","Time","Opening","TimeControl","GameTermination","GameMoves","ECO"]).agg(max("value"))
df_pvt_single_game.createOrReplaceTempView("games")


spark.sql("""SELECT * FROM games WHERE GameLink='[Site "https://lichess.org/e4gb7ja6"]'""").show(36,False)

#pivot test
#df_pvt_single_game.groupBy("Value").pivot("RowValue",["Event","GameLink"]).agg(max("value")).show()
#not working correctly. won't ever work??? Agg function and grouping will cause issues? Need to rely on order of rows


#try using SQL statements and min/max boundries for each game, 2nd table that I can join to
#more efficient row_num() calculation
#first/last functions?