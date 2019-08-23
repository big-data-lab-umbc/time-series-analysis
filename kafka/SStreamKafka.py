"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 Generate Prediction using Vector Auto Regression (VAR) on input streams from Kafka brokers.
 Stream processing framwork in Apache Spark Structed Streaming
 Usage: DStreamKafka.py <host:port> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      $SPARK_HOME/jars/spark-streaming-kafka-assembly-*.jar \
      ./VARonStreams/kafka/SStreamKafka.py \
      localhost:2181 test`
"""
import findspark
findspark.init()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pandas as pd
from CustomVAR import processStream

def foreach_batch_function(df, epoch_id):

# First splitting the value from Spark DF to get the timestamp from data and later applying window on the Date_Time

    split_col = F.split(df.value, ',')
    df = df.withColumn('datetime', to_timestamp(regexp_replace(split_col.getItem(0),'"',''),'yyyy-mm-dd HH:mm:ss'))
    df = df.withColumn('src1', split_col.getItem(1))
    df = df.withColumn('src2', regexp_replace(split_col.getItem(2), '"', ''),)
    df = df.drop('value')
    dfw = df.groupBy(window(df.datetime, "20 seconds", "10 seconds"),df.datetime,df.src1,df.src2).count()
    dfw = dfw.drop('count')
    dfw = dfw.drop('window')
    
    # Changing to Pandas dataframe
    pandadf = dfw.toPandas()

    # Running VAR on the window
    processStream(pandadf)

spark = SparkSession.builder.appName("TimeSeriesAnalytics").getOrCreate()
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "StreamVAR") \
    .load()

# lines.printSchema()

lines.selectExpr("CAST(value AS STRING)")

lines = lines.select(lines.value,lines.timestamp)

query = lines.writeStream.option('includeTimestamp', 'true').foreachBatch(foreach_batch_function).start()

query.awaitTermination()
