from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *
import pandas as pd
import pyspark.sql.functions as F

def foreach_batch_function(df, epoch_id):
	
	print('I am printing data!!')
	#print(df)
	#df.show()
	split_col = F.split(df.value, ',')
	df = df.withColumn('Date_Time', split_col.getItem(0))
	df = df.withColumn('Rt_avg', split_col.getItem(1))
	df = df.withColumn('Q_avg', split_col.getItem(2))
	df = df.withColumn('Rs_avg', split_col.getItem(3))
	df = df.withColumn('Rm_avg', split_col.getItem(4))
	df = df.withColumn('Ws_avg',split_col.getItem(5))
	df = df.withColumn('Nu_avg',split_col.getItem(6))
	df = df.drop('value')
	
	print(type(df))
	#df.show()
	
	#dataframe = df.select(F.window("Date_Time", "10 seconds","5 seconds"))
	#dataframe = df.select("Date_Time")
	pandadf = df.toPandas()
	print(pandadf)

spark = SparkSession.builder.appName("TurbineDataAnalytics").getOrCreate()

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 8885).load()

lines = lines.select(lines.value)

query = lines.writeStream.foreachBatch(foreach_batch_function).start()

query.awaitTermination()