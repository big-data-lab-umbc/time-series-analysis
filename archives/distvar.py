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
import pyspark.sql.functions
import pyspark.sql.functions as F
import pyspark.sql.types as tp
import numpy as np
import sys

# predictions on incoming streams
def pre_process(p_lag, dataFrame):
    print('###### Fitting VAR ######')
    print("Hi! Welcome to in fitVar select-order loop, current lag is ")
    print(p_lag)
    current_lag = p_lag

    # df_len_ori: number of variables in model, K
    x_list = dataFrame.columns
    print('x_list',x_list)
    df_len_ori = len(x_list)
    print("df_len_ori is ")
    print(df_len_ori)
    dataFrame_names = dataFrame.columns
    # dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    dataFrame.printSchema()
    dataFrame.show(10)
    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    # w = Window().partitionBy().orderBy(col("id"))
    w = Window().partitionBy().orderBy(col("Timestamp"))
    df_len = len(dataFrame.columns)
    print('df_lane',df_len)
    ys_lagged_list = ["const"]
    print('going in the lagged list')
    # Making sure first column is not considered for forecasting
    for i in range(1, p_lag + 1):
        for j in range(0, df_len):
            # making sure index column is not considered as feature column
            if x_list[j] != 'TimeStamp':
                ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
                # print('2',ys_lagged_list)
                dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
                # print('3')
    print("Showing DataFrame")
    dataFrame.show(5)
    print('ys_lagged_list',ys_lagged_list)

    # add "const" column of value 1 to get intercept when fitting the regression model
    dataFrame = dataFrame.withColumn("const", lit(1))
    dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
    #     for ii in range(0, df_len_ori):
    #         c_name_ii = "_c%s" % (str(ii))
    #         dataFrame = dataFrame.withColumn("_c%s-1" % (str(ii)), dataFrame[c_name_ii] - 1)

    # add "rid" column to control rollback process with p
    dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
    dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
    dataFrame.show(5)
    print("====added rid columns Pappu padoda ====")

    #     build ys_lagged dataframe, will be used in F-test
    ys_lagged = dataFrame.select(ys_lagged_list)
    ys_lagged_len = ys_lagged.count()
    print('ye dikhai lagged value')
    ys_lagged.show(10)

    #     dataFrame = dataFrame.drop('id')
    dataFrame = dataFrame.drop('rid')
    dataFrame = dataFrame.drop('const')
    return dataFrame

def fitVar(p_lag, dataFrame):
    print('###### Fitting VAR ######')
    print("Hi! Welcome to in fitVar select-order loop, current lag is ")
    print(p_lag)
    current_lag = p_lag

    # df_len_ori: number of variables in model, K
    x_list = dataFrame.columns
    print('x_list',x_list)
    df_len_ori = len(x_list)
    print("df_len_ori is ")
    print(df_len_ori)
    dataFrame_names = dataFrame.columns
    # dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    dataFrame.printSchema()
    dataFrame.show(10)
    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    # w = Window().partitionBy().orderBy(col("id"))
    w = Window().partitionBy().orderBy(col("Timestamp"))
    df_len = len(dataFrame.columns)
    print('df_lane',df_len)
    ys_lagged_list = ["const"]
    print('going in the lagged list')
    # Making sure first column is not considered for forecasting
    for i in range(1, p_lag + 1):
        for j in range(0, df_len):
            # making sure index column is not considered as feature column
            if x_list[j] != 'TimeStamp':
                ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
                # print('2',ys_lagged_list)
                dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
                # print('3')
    print("Showing DataFrame")
    dataFrame.show(5)
    print('ys_lagged_list',ys_lagged_list)

    # add "const" column of value 1 to get intercept when fitting the regression model
    dataFrame = dataFrame.withColumn("const", lit(1))
    dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
    #     for ii in range(0, df_len_ori):
    #         c_name_ii = "_c%s" % (str(ii))
    #         dataFrame = dataFrame.withColumn("_c%s-1" % (str(ii)), dataFrame[c_name_ii] - 1)

    # add "rid" column to control rollback process with p
    dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
    dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
    dataFrame.show(5)
    print("====added rid columns Pappu padoda ====")

    #     build ys_lagged dataframe, will be used in F-test
    ys_lagged = dataFrame.select(ys_lagged_list)
    ys_lagged_len = ys_lagged.count()
    print('ye dikhai lagged value')
    ys_lagged.show(10)

    #     dataFrame = dataFrame.drop('id')
    dataFrame = dataFrame.drop('rid')
    dataFrame = dataFrame.drop('const')
    input_feature_name = dataFrame.schema.names
    print('Phele ka pappu', input_feature_name)

    # input_feature_name.remove("id")
    for x_name in x_list:
        input_feature_name.remove('{}'.format(x_name))

    print('Baad ka pappu', input_feature_name)

    # assemble the vector for MLlib linear regression
    assembler_for_lag = VectorAssembler(
        inputCols=input_feature_name,
        outputCol="features")

    #     training_df = assembler_for_lag.transform(dataFrame)
    print('Pipeline Stages',assembler_for_lag)
    # select_y = x_list[0]
    # print(select_y)

    a = {}
    b = {}
    lrModels = []
    resids = []
    trainingSummaries = []
    # Arjun added this for evaluation
    evaluator = RegressionEvaluator()
    models = {}
    path = '../models/'
    for select_y in x_list:
        if select_y != 'TimeStamp':
            # model_key = 'model_{}'.format(select_y)
            model_key = '{}'.format(select_y)
            res_key = 'res_{}'.format(select_y)
            # Checking if model load works
            lr = LinearRegression(featuresCol='features', labelCol='{}'.format(select_y), maxIter=1000, fitIntercept=True)
            pipeline = Pipeline(stages=[assembler_for_lag, lr])
            model_val = pipeline.fit(dataFrame)
            # print("saving {} model".format(select_y))
            # model_val.save(path+'{}'.format(select_y))
            # print("showing fitted model")

            ##### Loading Model from file #####
            # print("Loading model for {}".format(select_y))
            # model_val = PipelineModel.load(path+'{}'.format(select_y))
            # print('1/2 way.... showing the dataframe passed for prediction')
            # exec ('df_model+'{}'.format(select_y) =
            # dataFrame.show(5)
            # model_val.transform(dataFrame).show()
            # print('Rabba ye to Chal gaya re....')
            #
            # predictions = model_val.transform(dataFrame)
            # predictions.show()
            # print("================{}===============".format(select_y))
            # print('Coefficients')
            # print(model_val.stages[1].coefficients)
            # print('Intercept')
            # print(model_val.stages[1].intercept)
            # print('Summary - RMSE')
            # # print(model_val.stages[1].summary.r2)
            # print('Prediction')
            # print(model_val.stages[1].prediction)

            # Arjun Added this code for the performance evaluation of the model
            # evaluator.setLabelCol("{}".format(select_y))
            # evaluator.setMetricName('mse')
            # evaluator.setPredictionCol("prediction")
            # print("Model performance")
            # score = evaluator.evaluate(model_val.transform(dataFrame))
            # print(score)
            models[model_key] = model_val
            # lrModels.append(model_val)
    # trying to return it as model
    # return lrModels #,predictions
    return models

def process(df, epoch_id):
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(F.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(tp.DoubleType()))
    df = df.withColumn('Nu_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(tp.DoubleType()))
    df = df.drop('value')
    dfw = df.select('TimeStamp','RT_Temp', 'Nu_Temp')
    l_lrmodels = []
    l_models = None
    l_predictions = None
    if len(dfw.take(1)) != 0:
        print('Pre-processing Data')
        dfp = pre_process(2,dfw)
        # l_lrmodels, l_predictions = fitVar(2,dfw)
        # fitting processed data
        l_models = fitVar(2, dfw)
        # print('Printing from outside')
        # l_predictions.show(5)
        # print('Showing selective columns')
        # l_predictions.selectExpr('TimeStamp', 'Nu_Temp as Nu_Temp_Actual', 'prediction as Nu_Temp_Predicted').show()
        print('Total Models',len(l_models))
        for key in l_models:
        # for i in range(0, len(l_models)):
           dfp = l_models[key].transform(dfp)
           dfp.selectExpr('TimeStamp', '{} as {}_Actual'.format(str(key),str(key)), 'prediction as {}_Predicted'.format(str(key))).show()

    ##### Checking dataM,.
    # pandadf.drop_duplicates(keep="first", inplace=True)
    currentDT = datetime.now()
    # results_fileName = '/afs/umbc.edu/users/a/p/apandya1/home/TSA/VARonStreams/sockets/Stream_Outputs/SStream/' + \
    #                    'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S") + '.csv'
    streamfile = '/Users/arjunpandya/PycharmProjects/sparkvar/'+ \
                       'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S") + '.csv'

host = 'localhost'
port = 8885
# if len(sys.argv) != 3:
#     print("Usage: saprk-submit SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
#     sys.exit(-1)
#
# broker = sys.argv[1]
# topic = sys.argv[2]

spark = SparkSession.builder.appName("TSF_SpeedLayer").enableHiveSupport().getOrCreate()


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .option("sep", ",") \
    .load()
# lines = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", broker) \
#     .option("startingOffsets", "earliest") \
#     .option("subscribe", topic) \
#     .load()
model_val = None
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime='30 seconds').foreachBatch(process).start()
query.awaitTermination()