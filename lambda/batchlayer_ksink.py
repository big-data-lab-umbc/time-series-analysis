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
from __future__ import print_function
from pyspark.sql import SparkSession
from var import fitVar
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, lit
import pyspark.sql.types as tp
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"

def recordstream(df, epoch_id):
    # First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime
    split_col = F.split(df.value, ',')
    # df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
    #                                                'yyyy-mm-dd HH:mm:ss.SSS'))
    df = df.withColumn('TimeStamp', F.regexp_replace(split_col.getItem(0), '"', '').cast(tp.TimestampType()))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    # Saving input stream to master data set
    dfw = df.selectExpr('TimeStamp as ts','RT_Temp','Nu_Temp')
    dfw.write.saveAsTable(name='tsa.turbine_master', format='hive', mode='append')
    dfp = df.select('TimeStamp','RT_Temp', 'Nu_Temp')

    if len(dfp.take(1)) != 0:
        # print('Calling Predictions & Model path is',g_model)
        df_final = fitVar(2,dfp,g_model)
        # df_final.show(5)
        # Converting selective columns from prediction dataframe to single column dataframe value
        df_final = df_final.withColumn('value',(F.concat(col("TS"),lit(","),
                                            col("RT_Temp"),lit(","),
                                            col("RT_Temp_Predict"),lit(","),
                                            col("Nu_Temp"), lit(","),
                                            col("Nu_Temp_Predict"), lit(","),
                                            col("RMSE_Score")
                                            )).cast(tp.StringType()))
        ds = df_final.select('value')
        # ds.show(5)
        # Sending each row of dataframe on Kafka message
        print('Now sending Message on Kafka topic',sink_topic)
        ds.selectExpr("CAST(value AS STRING)")\
            .write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", broker)\
            .option("topic", sink_topic)\
            .save()
        # df_final.write.saveAsTable(name='tsa.batch_predictions', format='hive', mode='append')



if len(sys.argv) != 6:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <source topic> <sink topic> <batch size> <model path>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
source_topic = sys.argv[2]
sink_topic = sys.argv[3]
batch_size = str(sys.argv[4]) + ' seconds'
g_model = str(sys.argv[5])

# host = 'localhost'
# port = 8885

spark = SparkSession.builder.appName("TSF_BatchLayer").enableHiveSupport().getOrCreate()

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", source_topic) \
    .load()
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime=batch_size).foreachBatch(recordstream).start()
query.awaitTermination()
