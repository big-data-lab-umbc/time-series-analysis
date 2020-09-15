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
from pyspark.sql.functions import lag, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
import numpy as np
import sys
def fitVar(p_lag, dataFrame):
    print('###### Fitting VAR ######')
    print("Hi! Welcome to in fitVar select-order loop, current lag is ")
    print(p_lag)
    current_lag = p_lag

    # df_len_ori: number of variables in model, K
    x_list = dataFrame.columns
    df_len_ori = len(x_list)
    print("df_len_ori is ")
    print(df_len_ori)
    dataFrame_names = dataFrame.columns
    dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    dataFrame.printSchema()
    dataFrame.show(10)
    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    w = Window().partitionBy().orderBy(col("id"))
    df_len = len(dataFrame.columns)
    ys_lagged_list = ["const"]
    for i in range(1, p_lag + 1):
        for j in range(0, df_len - 1):
            ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
            dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
    print("!!!!!!!!1")
    dataFrame.show(5)
    print(ys_lagged_list)

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

    input_feature_name.remove("id")
    for x_name in x_list:
        input_feature_name.remove('{}'.format(x_name))

    print('Baad ka pappu', input_feature_name)

    # assemble the vector for MLlib linear regression
    assembler_for_lag = VectorAssembler(
        inputCols=input_feature_name,
        outputCol="features")

    #     training_df = assembler_for_lag.transform(dataFrame)

    select_y = x_list[0]
    print(select_y)

    a = {}
    b = {}
    lrModels = []
    resids = []
    trainingSummaries = []
    # Arjun added this for evaluation
    evaluator = RegressionEvaluator()

    path ='./models/'
    for select_y in x_list:
        model_key = 'model_{}'.format(select_y)
        res_key = 'res_{}'.format(select_y)
        # Checking if model load works
        lr = LinearRegression(featuresCol='features', labelCol='{}'.format(select_y), maxIter=1000, fitIntercept=True)
        pipeline = Pipeline(stages=[assembler_for_lag, lr])
        model_val = pipeline.fit(dataFrame)
        print("saving {} model".format(select_y))
        # model_val.save(path+'{}'.format(select_y))
        model_val.write().overwrite().save(path+'{}'.format(select_y))
        print("showing fitted model")

        ### Loading Model from file #####
        print("Loading model for {}".format(select_y))
        model_val = PipelineModel.load(path+'{}'.format(select_y))
        print('1/2 way....')
        model_val.transform(dataFrame).show()
        print('Rabba ye to Chal gaya re....')

        predictions = model_val.transform(dataFrame)
        predictions.show()
        print("================{}===============".format(select_y))
        print('Coefficients')
        print(model_val.stages[1].coefficients)
        print('Intercept')
        print(model_val.stages[1].intercept)
        print('Summary - RMSE')
        # print(model_val.stages[1].summary.r2)
        # print('Prediction')
        # print(model_val.stages[1].prediction)

        # Arjun Added this code for the performance evaluation of the model
        evaluator.setLabelCol("{}".format(select_y))
        evaluator.setMetricName('mse')
        evaluator.setPredictionCol("prediction")
        print("Model performance")
        score = evaluator.evaluate(model_val.transform(dataFrame))
        print(score)

        trainingSummaries.append(model_val.stages[1].summary)
        model_val.stages[1].summary.residuals.show(10)
        print(model_val.stages[1].summary.residuals.count())
        residual_val = np.array(model_val.stages[1].summary.residuals.select('residuals').collect()).flatten()
        #         print(residual_val)
        a[model_key] = model_val
        b[res_key] = residual_val
        #         print(b)
        lrModels.append(model_val)
        resids.append(residual_val)
    result_len = len(lrModels[0].stages[1].coefficients)
    #     print(result_len)

    cov_resids = np.cov(resids)
    print("cov_resids is")
    print(cov_resids)

    # degree of freedom of cov_resids
    obs = ys_lagged.count()
    # print("obs is")
    # print(obs)
    df_cov_resids = obs - ((len(ys_lagged.columns) + df_len_ori) - df_len_ori)
    # print("len(ys_lagged.columns) is" )
    # print(len(ys_lagged.columns))
    # print("len(ys_lagged.columns) + df_len_ori   is ")
    # print(len(ys_lagged.columns) + df_len_ori)
    # print("df_cov_resids is")
    # print(df_cov_resids)
    # covres is sigma_u
    covres = (cov_resids * (obs - 1)) / df_cov_resids
    print("covres is ")
    print(covres)

    # =====================`
    # build VAR result matrix
    print("====start building tsa result mtx====")
    result_mtx = []
    result_len = len(lrModels[0].stages[1].coefficients)
    print("result length is")
    print(result_len)
    for b in range(0, df_len_ori):
        result_arr = []
        result_arr.append(lrModels[b].stages[1].intercept)
        for a in range(0, result_len):
            result_arr.append(lrModels[b].stages[1].coefficients[a])
        result_mtx.append(result_arr)
    result_mtx = np.array(result_mtx)
    print(result_mtx)

    # print(result_mtx)
    # print("========len(result_mtx)=======")
    # print(len(result_mtx))
    # print("===============")
    # dataFrame.unpersist()
    print("now I'm building the return list :)")
    print("first return value is result_mtx, which is ")
    print(result_mtx)
    # print("dataFrame_names is ")
    # print(dataFrame_names)
    # print("df_len_ori")
    # print(df_len_ori)
    # print("current_lag")
    # print(current_lag)
    # print("ys_lagged")
    # print(ys_lagged)
    # print(result_mtx, dataFrame_names, df_len_ori, current_lag, ys_lagged, covres, df_cov_resids, ys_lagged_len)
def foreach_batch_function(df, epoch_id):
    # First splitting the value from Spark DF to get the timestamp from data and later applying window on the datetime
    #     print(df)
    split_col = F.split(df.value, ',')
    df = df.withColumn('TimeStamp', F.to_timestamp(pyspark.sql.functions.regexp_replace(split_col.getItem(0), '"', ''),
                                                   'yyyy-mm-dd HH:mm:ssss'))
    df = df.withColumn('RT_Temp', split_col.getItem(1).cast(T.DoubleType()))
    df = df.withColumn('Nu_Temp', F.regexp_replace(split_col.getItem(2), '"', '').cast(T.DoubleType()))
    df = df.drop('value')
    # dfw = df.select('TimeStamp','RT_Temp','ST_Temp')
    # pandadf = dfw.toPandas()
    dfw = df.select('RT_Temp', 'Nu_Temp')
    # df2 = dfw['RT_Temp'].cast(T.DoubleType())
    # dfw = df.selectExpr("cast (RT_Temp as double) RT Temp","cast (ST_Temp as double) ST Temp")
    # dfw = df.select()
    if len(dfw.take(1)) != 0:
        print('Now Creating VAR Model')
        fitVar(2,dfw)
    ##### Checking dataM,.
    # pandadf.drop_duplicates(keep="first", inplace=True)
    currentDT = datetime.now()
    # results_fileName = '/afs/umbc.edu/users/a/p/apandya1/home/TSA/VARonStreams/sockets/Stream_Outputs/SStream/' + \
    #                    'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S") + '.csv'
    streamfile = '/Users/arjunpandya/PycharmProjects/sparkvar/'+ \
                       'SStream_at_' + currentDT.strftime("%Y-%m-%d %H%M%S") + '.csv'


# if len(sys.argv) != 3:
#     print("Usage: SStreamKafka.py <hostname:port> <topic>", file=sys.stderr)
#     sys.exit(-1)


host = 'localhost'
port = 8885

spark = SparkSession.builder.appName("TimeSeriesAnalytics").getOrCreate()
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .option("sep", ",") \
    .load()
spark.sparkContext.setLogLevel("FATAL")
query = lines.writeStream.trigger(processingTime='30 seconds').foreachBatch(foreach_batch_function).start()
query.awaitTermination()
