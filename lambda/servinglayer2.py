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
    # df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
    #                                                'yyyy-mm-dd HH:mm:ss.SSS'))
    df = df.withColumn('TimeStamp', F.regexp_replace(split_col.getItem(0), '"', '').cast(tp.TimestampType()))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('RT_Temp_Predict', split_col.getItem(2).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', split_col.getItem(3).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp_Predict', split_col.getItem(4).cast(tp.DoubleType()))
    df = df.withColumn('RMSE_Score', F.regexp_replace(split_col.getItem(4), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    # df.show()
    sp_df = df.select('TimeStamp','RT_Temp','RT_Temp_Predict','Nu_Temp','Nu_Temp_Predict','RMSE_Score')\
        .where("topic='{}'".format(str(sp_topic)))
    bt_df = df.select('TimeStamp','RT_Temp','RT_Temp_Predict','Nu_Temp','Nu_Temp_Predict','RMSE_Score')\
        .where("topic='{}'".format(str(bl_topic)))
    # print("Speed Layer Predictions....")
    # sp_df.show(5)
    # print("Batch Layer Predictions....")
    # bt_df.show(5)

    df_final = (
        sp_df.alias('sp').join(bt_df.alias('bt'), on=sp_df['TimeStamp'] == bt_df['TimeStamp'],
                                    how='inner').selectExpr('sp.TimeStamp as TS',
                                                            'round(sp.RT_Temp,3) as RT_Temp',
                                                            'round(sp.RT_Temp_Predict,3) as Speed_RT_Temp',
                                                            'round(bt.RT_Temp_Predict,3) as Batch_RT_Temp',
                                                            'round(({}*sp.RT_Temp_Predict + {}*bt.RT_Temp_Predict),3) as Wt_RT_Temp'.format(str(s_wt),str(b_wt)),
                                                            'round(sp.Nu_Temp,3) as Nu_Temp',
                                                            'round(sp.Nu_Temp_Predict,3) as Speed_Nu_Temp',
                                                            'round(bt.Nu_Temp_Predict,3) as Batch_Nu_Temp',
                                                            'round(({}*sp.Nu_Temp_Predict + {}*bt.Nu_Temp_Predict),3) as Wt_Nu_Temp'.format(str(s_wt),str(b_wt)),
                                                            'round(sp.RMSE_Score,3) as Speed_RMSE',
                                                            'round(bt.RMSE_Score,3) as Batch_RMSE'
                                                            )
                )
    df_final.show(5)
    # df = spark.sql("select * FROM default.turbine")
    df_final.write.saveAsTable(name='tsa.serving_predictions', format='hive', mode='append')

# host = 'localhost'
# port = 8885
if len(sys.argv) != 7:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <speed topic> "
          "<batch topic> <batch size> <weight for batch prediction> <weight for speed prediction>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
sp_topic = sys.argv[2]
bl_topic = sys.argv[3]
batch_size = str(sys.argv[4]) + ' seconds'
s_wt = sys.argv[5] # Weight assigned to prediction from Speed Layer
b_wt = sys.argv[6] # Weight assigned to prediction from Batch Layer

spark = SparkSession.builder.appName("TSF_ServingLayer").enableHiveSupport().getOrCreate()

predictions = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", str(sp_topic)+','+str(bl_topic)) \
    .load()
model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = predictions.writeStream.trigger(processingTime=batch_size).foreachBatch(weighted_predict).start()
query.awaitTermination()