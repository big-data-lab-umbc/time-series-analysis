from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
from var import fitVar
import pyspark.sql.functions
import pyspark.sql.functions as F
import pyspark.sql.types as tp
import numpy as np
import sys
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell"

def predict(df, epoch_id):
    split_col = F.split(df.value, ',')
    # df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
    #                                                'yyyy-mm-dd HH:mm:ss.SSS'))
    df = df.withColumn('TimeStamp', F.regexp_replace(split_col.getItem(0), '"', '').cast(tp.TimestampType()))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    dfw = df.select('TimeStamp','RT_Temp', 'Nu_Temp')

    if len(dfw.take(1)) != 0:
        df_final = fitVar(2, dfw)
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
        print('Now sending Kafka Message')
        ds.selectExpr("CAST(value AS STRING)")\
            .write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", broker)\
            .option("topic", sink_topic)\
            .save()
        # Saving the predicted values on Hive for recording purpose
        # df_final.write.saveAsTable(name='tsa.speed_predictions', format='hive', mode='append')

if len(sys.argv) != 5:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
source_topic = sys.argv[2]
sink_topic = sys.argv[3]
batch_size = str(sys.argv[4]) + ' seconds'

spark = SparkSession.builder.appName("TSF_SpeedLayer").enableHiveSupport().getOrCreate()

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", source_topic) \
    .load()

model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime=batch_size).foreachBatch(predict).start()
query.awaitTermination()