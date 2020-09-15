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
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"
# from CustomVAR import processStream
from tsa.var import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
import sys
# from VAR import *
import warnings
warnings.filterwarnings("ignore")

def foreach_batch_function(df, epoch_id):
    # First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime
    #     print(df)
    split_col = F.split(df.value, ',')
    df = df.withColumn('TS', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    # df = df.withColumn('TS', F.to_timestamp(pyspark.sql.functions.regexp_replace(split_col.getItem(0), '"', ''),
    #                                                'YYYY-MM-DD HH:MM:SS.fffffffff'))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(T.DoubleType()))
    df = df.withColumn('ST_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(T.DoubleType()))
    df = df.drop('value')
    dfw = df.select('TS','RT_Temp','ST_Temp')
    print(dfw)

    # df2 = df.select('RT_Temp','ST_Temp')
    dfw.write.saveAsTable(name='default.turbine', format='hive', mode='append')
    dfw = df.select('RT_Temp', 'ST_Temp')
    if len(dfw.take(1)) != 0:
        print('Now Creating VAR Model')
        fitVar(2,dfw)

if len(sys.argv) != 3:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
topic = sys.argv[2]

# host = 'localhost'
# port = 8885

spark = SparkSession.builder.appName("TimeSeriesAnalytics-VAR").enableHiveSupport().getOrCreate()

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", topic) \
    .load()
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime='30 seconds').foreachBatch(foreach_batch_function).start()
query.awaitTermination()
