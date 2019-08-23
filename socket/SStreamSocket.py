"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 This program uses Spark Structured Streaming Counts words in UTF8 encoded, '\n' delimited text received from the network every 10 second.
 Usage: SStreamSocket.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run
   `$ bin/spark-submit ../VARonStreaming/DatageneratorSocket.py localhost <port>`
 and then run
    `$ bin/spark-submit ../VARonStreaming/SStreamSocket.py localhost <port>`
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from CustomVAR import processStream
import pyspark.sql.functions as F
import pandas as pd
import sys

def foreach_batch_function(df, epoch_id):

# First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime

    split_col = F.split(df.value, ',')
    df = df.withColumn('datetime', to_timestamp(regexp_replace(split_col.getItem(0),'"',''),'yyyy-mm-dd HH:mm:ss'))
    df = df.withColumn('src1', split_col.getItem(1))
    df = df.withColumn('src2', regexp_replace(split_col.getItem(2), '"', ''),)
    df = df.drop('value')

    dfw = df.groupBy(window(df.datetime, "20 seconds", "10 seconds"),df.datetime,df.src1,df.src2).count()
    dfw = dfw.drop('count')
    dfw = dfw.drop('window')
    pandadf = dfw.toPandas()

    # Running VAR on the window
    if not pandadf.empty:
        processStream(pandadf)

if len(sys.argv) != 3:
        print("Usage: SStreamSocket.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

spark = SparkSession.builder.appName("TimeSeriesAnalytics").getOrCreate()
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", sys.argv[1]) \
    .option("port", int(sys.argv[2])) \
    .load()
lines.selectExpr("CAST(value AS STRING)")
# lines = lines.select(lines.value)
query = lines.writeStream.foreachBatch(foreach_batch_function).start()
query.awaitTermination()
