import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math as m
from sklearn.metrics import mean_squared_error
import datetime



df = pd.read_csv("panda_test_file.csv")

#df.dtypes

def process(df):

    df['Date_Time'] = pd.to_datetime(df.Date_Time , format = '%m/%d/%Y %H:%M')
    data = df.drop(['Date_Time'], axis=1)
    data.index = df.Date_Time
    print('date time indexing done!-------------------------------------')

    cols = data.columns
    for j in cols:
        for i in range(0,len(data)):
           if pd.isnull(data[j][i]):
               data[j][i] = data[j][i-1]

    #creating the train and validation set
    train = data[:int(0.8*(len(data)))]
    valid = data[int(0.8*(len(data))):]

    print(train)
    print(valid)

    #fit the model
    from statsmodels.tsa.vector_ar.var_model import VAR

    model = VAR(endog=train.astype(float))
    model_fit = model.fit()

    # make prediction on validation
    prediction = model_fit.forecast(model_fit.y, steps=len(valid))

    #print(prediction)

    #open file to write results
    currentDT = datetime.datetime.now()
    results_fileName = 'Predictions_at_' + currentDT.strftime("%Y-%m-%d %H:%M:%S")
    f = open("/home/chai2/jianwu_common/Time-Series-IoT-Analytics-Chaitanya/spark-2.4.0-bin-hadoop2.7/%s.txt" %results_fileName, "a+")
    
    
    #converting predictions to dataframe
    pred = pd.DataFrame(index=range(0,len(prediction)),columns=[cols])
    for j in range(0,6):
        for i in range(0, len(prediction)):
           pred.iloc[i][j] = prediction[i][j]

    #check rmse
    for i in cols:
        results_fileName = 'rmse value for ' + str(i) + ' is : ' + str(m.sqrt(mean_squared_error(pred[i], valid[i]))) + "\n"
        #print('rmse value for', i, 'is : ', m.sqrt(mean_squared_error(pred[i], valid[i])))
        #print(result)
        f.write(results_fileName)

    model = VAR(endog=data.astype(float))
    model_fit = model.fit()
    yhat = model_fit.forecast(model_fit.y, steps=2)
    print(yhat)