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

# predictions on incoming streams
# def fitVar(p_lag, dataFrame):
#
#     # print(p_lag)
#     current_lag = p_lag
#
#     # df_len_ori: number of variables in model, K
#     x_list = dataFrame.columns
#     # print('x_list',x_list)
#     df_len_ori = len(x_list)
#     # print("df_len_ori is ")
#     # print(df_len_ori)
#     dataFrame_names = dataFrame.columns
#     # dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
#     dataFrame.printSchema()
#     # dataFrame.show(10)
#     # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
#     # w = Window().partitionBy().orderBy(col("id"))
#     w = Window().partitionBy().orderBy(col("Timestamp"))
#     df_len = len(dataFrame.columns)
#     ys_lagged_list = ["const"]
#     # Making sure first column is not considered for forecasting
#     for i in range(1, p_lag + 1):
#         for j in range(0, df_len):
#             # making sure index column is not considered as feature column
#             if x_list[j] != 'TimeStamp':
#                 ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
#                 # print('2',ys_lagged_list)
#                 dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
#                 # print('3')
#     # print("Showing DataFrame")
#     # dataFrame.show(5)
#     # print('ys_lagged_list',ys_lagged_list)
#
#     # add "const" column of value 1 to get intercept when fitting the regression model
#     dataFrame = dataFrame.withColumn("const", lit(1))
#     dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
#     dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
#     dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
#     # dataFrame.show(5)
#     #     build ys_lagged dataframe, will be used in F-test
#     ys_lagged = dataFrame.select(ys_lagged_list)
#     ys_lagged_len = ys_lagged.count()
#     # print('ye dikhai lagged value')
#     # ys_lagged.show(10)
#
#     #     dataFrame = dataFrame.drop('id')
#     dataFrame = dataFrame.drop('rid')
#     dataFrame = dataFrame.drop('const')
#     input_feature_name = dataFrame.schema.names
#
#     # input_feature_name.remove("id")
#     for x_name in x_list:
#         input_feature_name.remove('{}'.format(x_name))
#
#     # assemble the vector for MLlib linear regression
#     assembler_for_lag = VectorAssembler(
#         inputCols=input_feature_name,
#         outputCol="features")
#
#     # a = {}
#     # b = {}
#     lrModels = []
#     # Arjun added this for evaluation
#     evaluator = RegressionEvaluator()
#     models = {}
#     predictions = {}
#     # path ='./models/'
#     for select_y in x_list:
#         if select_y != 'TimeStamp':
#             model_key = '{}'.format(select_y)
#             res_key = 'res_{}'.format(select_y)
#             # Checking if model load works
#             lr = LinearRegression(featuresCol='features', labelCol='{}'.format(select_y), maxIter=1000, fitIntercept=True)
#             pipeline = Pipeline(stages=[assembler_for_lag, lr])
#             model_val = pipeline.fit(dataFrame)
#             # print("saving {} model".format(select_y))
#             # model_val.save(path+'{}'.format(select_y))
#             # print("showing fitted model")
#             # print("================{}===============".format(select_y))
#             # print('Coefficients')
#             # print(model_val.stages[1].coefficients)
#             # print('Intercept')
#             # print(model_val.stages[1].intercept)
#
#             # Arjun Added this code for the performance evaluation of the model
#             evaluator.setLabelCol("{}".format(select_y))
#             # Root Mean Square Error
#             evaluator.setMetricName('rmse')
#             evaluator.setPredictionCol("prediction")
#             rmse = evaluator.evaluate(model_val.transform(dataFrame))
#             # Mean Square Error
#             evaluator.setMetricName('mse')
#             mse = evaluator.evaluate(model_val.transform(dataFrame))
#             # Mean Absolute Error
#             evaluator.setMetricName('mae')
#             mae = evaluator.evaluate(model_val.transform(dataFrame))
#
#             models[model_key] = model_val
#             predictions[model_key] = model_val.transform(dataFrame)
#             lrModels.append(model_val)
#     # trying to return it as model
#     # return lrModels,predictions
#
#     df_RT_Temp = predictions['RT_Temp']
#     df_Nu_Temp = predictions['Nu_Temp']
#     df_final = (df_RT_Temp.alias('dr').join(df_Nu_Temp.alias('dn'), on=df_RT_Temp['TimeStamp']== df_Nu_Temp['TimeStamp'],
#                                             how='inner').selectExpr('dr.TimeStamp as TS',
#                                                                     'dr.RT_Temp',
#                                                                     'dr.prediction as RT_Temp_Predict',
#                                                                     'dn.Nu_Temp as Nu_Temp',
#                                                                     'dn.prediction as NU_Temp_Predict')
#                 )
#     df_final = df_final.withColumn("MAE_Score", lit(mae))
#     df_final = df_final.withColumn("MSE_Score", lit(mse))
#     df_final = df_final.withColumn("RMSE_Score", lit(rmse))
#     df_final.show()
#     df_final.write.saveAsTable(name='tsa.speed_predictions', format='hive', mode='append')
#     # return lrModels

def predict(df, epoch_id):
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    dfw = df.select('TimeStamp','RT_Temp', 'Nu_Temp')

    if len(dfw.take(1)) != 0:
        df_final = fitVar(2, dfw)
        df_final.write.saveAsTable(name='tsa.speed_predictions', format='hive', mode='append')

# host = 'localhost'
# port = 8885
if len(sys.argv) != 4:
    print("Usage: saprk-submit SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
    sys.exit(-1)

broker = sys.argv[1]
topic = sys.argv[2]
batch_size = str(sys.argv[3]) + ' seconds'

spark = SparkSession.builder.appName("TSF_SpeedLayer").enableHiveSupport().getOrCreate()

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", broker) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", topic) \
    .load()

model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime=batch_size).foreachBatch(predict).start()
query.awaitTermination()