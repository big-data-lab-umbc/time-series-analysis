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
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('RT_Temp_Predict', split_col.getItem(2).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', split_col.getItem(3).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp_Predict', F.regexp_replace(split_col.getItem(4), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    sp_df = df.select('TimeStamp','RT_Temp','RT_Temp_Predict','Nu_Temp','Nu_Temp_Predict')\
        .where("topic='speed_predictoins'")
    bt_df = df.select('TimeStamp', 'RT_Temp', 'RT_Temp_Predict', 'Nu_Temp', 'Nu_Temp_Predict')\
        .where("topic='batch_predictoins'")
    print("Speed Layer Predictions....")
    sp_df.show(5)
    print("Batch Layer Predictions....")
    bt_df.show(5)

    df_final = (
        sp_df.alias('sp').join(bt_df.alias('bt'), on=sp_df['TimeStamp'] == bt_df['TimeStamp'],
                                    how='inner').selectExpr('sp.TimeStamp as TS',
                                                            'sp.RT_Temp',
                                                            'sp.RT_Temp_Predict as Speed_RT_Temp',
                                                            'bt.RT_Temp_Predict as Batch_RT_Temp',
                                                            '(0.2*sp.RT_Temp_Predict + 0.8*bt.RT_Temp_Predict) as Y_RT_Temp',
                                                            'sp.Nu_Temp',
                                                            'sp.Nu_Temp_Predict as Speed_Nu_Temp',
                                                            'bt.Nu_Temp_Predict as Batch_Nu_Temp',
                                                            '(0.2*sp.Nu_Temp_Predict + 0.8*bt.Nu_Temp_Predict) as Y_Nu_Temp'
                                                            )
                )
    df_final.show(5)
    # df = spark.sql("select * FROM default.turbine")
    df_final.write.saveAsTable(name='tsa.serving_predictions', format='hive', mode='append')

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
    .option("startingOffsets", "latest") \
    .option("subscribe", 'speed_predictoins,batch_predictoins') \
    .load()
model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = speed_predictoins.writeStream.trigger(processingTime=batch_size).foreachBatch(weighted_predict).start()
query.awaitTermination()