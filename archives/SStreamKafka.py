"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 03/21/2020

 This program uses Spark Structured Streaming Counts words in UTF8 encoded, '\n' delimited text received from the network every 10 second.
 Usage: DStreamSocket.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run
   `$ bin/spark-submit ../VARonStreaming/DatageneratorSocket.py <localhost> <port>`
 and then run
    `$ bin/spark-submit ../VARonStreaming/socket/StreamSocket.py <localhost> <port>`
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"
from CustomVAR import processStream
import pyspark.sql.functions as F
import pandas as pd
import sys
from datetime import datetime
# from VAR import *
import warnings
warnings.filterwarnings("ignore")

def foreach_batch_function(df, epoch_id):
    # First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime
    #     print(df)
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(pyspark.sql.functions.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1))
    df = df.withColumn('ST_Temp', F.regexp_replace(split_col.getItem(2), '"', ''), )
    df = df.drop('value')
    dfw = df.select('TimeStamp','RT_Temp','ST_Temp')
    pandadf = dfw.toPandas()

    ##### Checking dataM,.
    # pandadf.drop_duplicates(keep="first", inplace=True)
    currentDT = datetime.now()
    results_fileName = '/afs/umbc.edu/users/a/p/apandya1/home/TSA/VARonStreams/sockets/Stream_Outputs/SStream/' + \
                       'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S") + '.csv'
    pandadf.to_csv(results_fileName)
    #
    # # Running VAR on the batch
    if not pandadf.empty:
        processStream(pandadf, 'SStream')

if len(sys.argv) != 3:
    print("Usage: SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
topic = sys.argv[2]

# host = 'localhost'
# port = 8885

spark = SparkSession.builder.appName("TimeSeriesAnalytics").getOrCreate()
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", topic) \
    .load()

query = lines.writeStream.trigger(processingTime='30 seconds').foreachBatch(foreach_batch_function).start()
query.awaitTermination()
