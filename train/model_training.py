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
import numpy as np
import sys


def train(p_lag, dataFrame, model_path=None):
    # print(p_lag)
    current_lag = p_lag

    # df_len_ori: number of variables in model, K
    x_list = dataFrame.columns
    # print('x_list',x_list)
    df_len_ori = len(x_list)
    # print("df_len_ori is ")
    # print(df_len_ori)
    dataFrame_names = dataFrame.columns
    dataFrame = dataFrame.withColumn("id", monotonically_increasing_id())
    # dataFrame.printSchema()
    # dataFrame.show(10)
    # Here, VAR model regression_type is "const" same to R VAR library, and the default in Python VAR library
    # w = Window().partitionBy().orderBy(col("id"))
    w = Window().partitionBy().orderBy(col("id"))
    df_len = len(dataFrame.columns)
    ys_lagged_list = ["const"]
    # Making sure first column is not considered for forecasting
    for i in range(1, p_lag + 1):
        for j in range(0, df_len - 1):
            # making sure index column is not considered as feature column
            if x_list[j] != 'TimeStamp':
                ys_lagged_list.append("%st-%s" % (x_list[j], str(i)))
                print('2',ys_lagged_list)
                dataFrame = dataFrame.withColumn("%st-%s" % (x_list[j], str(i)), lag(dataFrame[j], i, 0).over(w))
                # print('3')
    # print("Showing DataFrame")
    dataFrame.show(5)
    print('ys_lagged_list',ys_lagged_list)

    # add "const" column of value 1 to get intercept when fitting the regression model
    dataFrame = dataFrame.withColumn("const", lit(1))
    dataFrame = dataFrame.withColumn("const", lag("const", p_lag, 0).over(w))
    dataFrame = dataFrame.withColumn("rid", monotonically_increasing_id())
    dataFrame = dataFrame.filter(dataFrame.rid >= p_lag)
    # dataFrame.show(5)
    #     build ys_lagged dataframe, will be used in F-test
    ys_lagged = dataFrame.select(ys_lagged_list)
    ys_lagged_len = ys_lagged.count()
    # print('ye dikhai lagged value')
    # ys_lagged.show(10)

    dataFrame = dataFrame.drop('id')
    dataFrame = dataFrame.drop('rid')
    dataFrame = dataFrame.drop('const')
    input_feature_name = dataFrame.schema.names

    # input_feature_name.remove("id")
    for x_name in x_list:
        input_feature_name.remove('{}'.format(x_name))

    # assemble the vector for MLlib linear regression
    assembler_for_lag = VectorAssembler(
        inputCols=input_feature_name,
        outputCol="features")

    # a = {}
    # b = {}
    models = {}
    lrModels = []
    # print('Iterating the features')
    for select_y in x_list:
        if select_y != 'TimeStamp':
            model_key = '{}'.format(select_y)
            # ML model will be trained for each micro batch if existing model is not provided
            # print('model path',model_path+ '{}'.format(select_y))
            lr = LinearRegression(featuresCol='features', labelCol='{}'.format(select_y), maxIter=1000,
                                  fitIntercept=True)
            pipeline = Pipeline(stages=[assembler_for_lag, lr])
            model_val = pipeline.fit(dataFrame)
            # model_val.write().overwrite().save(model_path+'{}'.format(select_y))
            lrModels.append('{}'.format(select_y))
            models ['{}'.format(select_y)] = model_val
    return lrModels, models
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SparkVarTraining") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    # p_lag: time lag
    # p_lag = int(sys.argv[1])
    # data_file_name = sys.argv[2]
    # model_path = str(sys.argv[3])
    p_lag = 2
    data_file_name = '../source/autoregsrc.csv'
    model_path = '../models/'
    dataFrame = spark.read.csv(data_file_name, header=True, inferSchema=True)
    lr_model = []
    model = {}
    lr_model, model = train(p_lag, dataFrame, model_path)
    # saving trained model for each endogenous variable in VAR
    for i in range(0,len(lr_model)):
        print('lr_model',lr_model[i])
        trained_model = model[lr_model[i]]
        trained_model.write().overwrite().save(model_path+'{}'.format(str(lr_model[i])))
