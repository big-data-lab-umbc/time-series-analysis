from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col
from pyspark.sql.functions import lit
from datetime import datetime
import pyspark.sql.functions
import pyspark.sql.functions as F
import pyspark.sql.types as tp
import numpy as np
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"

# predictions on incoming streams
def weighted_predict(df, epoch_id):
    # print(df.value)
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('SP_RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('SP_RT_Temp_Predict', split_col.getItem(2).cast(tp.DoubleType()))
    df = df.withColumn('SP_Nu_Temp', split_col.getItem(3).cast(tp.DoubleType()))
    df = df.withColumn('SP_Nu_Temp_Predict', F.regexp_replace(split_col.getItem(4), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    sp_df = df.select('TimeStamp','SP_RT_Temp','SP_RT_Temp_Predict','SP_Nu_Temp','SP_Nu_Temp_Predict')
    print("Speed Layer Predictions....")
    sp_df.show(5)
    p_start_dt = sp_df.agg({'TimeStamp':'max'})
    p_end_dt = sp_df.agg({'TimeStamp':'min'})
    p_start_dt.show()
    p_end_dt.show()
    print('EndTime', p_end_dt)
    bt_df = spark.sql("select * FROM tsa.batch_predictions limit 10")
    print("Batch Layer Predictions....")
    bt_df.show(5)

    # df = spark.sql("select * FROM default.turbine")
    # df_final.write.saveAsTable(name='tsa.speed_predictions', format='hive', mode='append')

# host = 'localhost'
# port = 8885
if len(sys.argv) != 4:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
topic = sys.argv[2]
batch_size = str(sys.argv[3]) + ' seconds'

spark = SparkSession.builder.appName("TSF_ServingLayer").enableHiveSupport().getOrCreate()

# batch_predictoins = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", broker) \
#     .option("startingOffsets", "earliest") \
#     .option("subscribe", 'batch_predictoins') \
#     .load()

speed_predictoins = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", 'speed_predictoins') \
    .load()
model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = speed_predictoins.writeStream.trigger(processingTime=batch_size).foreachBatch(weighted_predict).start()
query.awaitTermination()