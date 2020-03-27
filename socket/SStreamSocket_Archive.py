from pyspark.sql import SparkSession
import pyspark.sql.functions
# from CustomVAR import processStream
from CustomVAR import processStream
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql.types import DateType
import pandas as pd
import sys
from datetime import datetime

def foreach_batch_function(df, epoch_id):

# First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime
    print(df)
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(pyspark.sql.functions.regexp_replace(split_col.getItem(0), '"', ''), 'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1))
    df = df.withColumn('ST_Temp', F.regexp_replace(split_col.getItem(2), '"', ''), )
    df = df.drop('value')

    dfw = df.withWatermark('TimeStamp', "30 seconds").groupBy(pyspark.sql.functions.window('TimeStamp', "30 seconds", "10 seconds"), df.TimeStamp, df.RT_Temp, df.ST_Temp).count()
    dfw = dfw.drop('count')
    dfw = dfw.drop('window')
    # dfw = dfw.dropDuplicates("TimeStamp")
    pandadf = dfw.toPandas()
    # pandadf = pandadf.drop()

##### Checking dataM,.
    pandadf.drop_duplicates(keep="first", inplace=True)
    currentDT = datetime.now()
    results_fileName = '/Users/arjunpandya/PycharmProjects/VARonStreams/sockets/Stream_Outputs/SStream/'+\
        'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S")+'.csv'
    pandadf.to_csv(results_fileName)
    #
    # # Running VAR on the window
    if not pandadf.empty:
        processStream(pandadf,'SStream')

# if len(sys.argv) != 3:
#         print("Usage: SStreamSocket_Archive.py <hostname> <port>", file=sys.stderr)
#         sys.exit(-1)

spark = SparkSession.builder.appName("TimeSeriesAnalytics").getOrCreate()

# defining schema of the incoming data
# userSchema = StructType()\
#         .add("timestamp", DateType)\
#         .add("RT_Temp", DoubleType)\
#         .add("ST_Temp", DoubleType)

userSchema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("RT_Temp", DoubleType(), True),
    StructField("ST_Temp", DoubleType(), True)
])

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8885) \
    .option("includeTimestamp", True)\
    .option("sep", ",")\
    .load()

windowedline = lines.withWatermark('timestamp', "30 seconds").groupby(F.window(lines.timestamp, "30 seconds", "10 seconds"), lines.value).count()
# windowedline = windowedline.withWatermark("timestamp", "30 seconds") .dropDuplicates("timestamp", "value")
# windowedline = windowedline.dropDuplicates("value")
# lines.selectExpr("CAST(value AS STRING)")
# lines = lines.select(lines.value)
# query = lines.writeStream.foreachBatch(foreach_batch_function).start()
# windoweddf = F.window("Batchfff ",30,20)
# print("Incoming line")
# windowedline.pprint()
# query = windowedline.writeStream.format("console").outputMode("complete").start()
query = windowedline.writeStream.trigger(processingTime='30 seconds').foreachBatch(foreach_batch_function).outputMode("complete").start()
query.awaitTermination()
# query.awaitTermination()