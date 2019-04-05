import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math as m
from sklearn.metrics import mean_squared_error

df = pd.read_csv("/home/chaitanya/Downloads/Only-R80711-SC.csv")

df.dtypes

df['Date_Time'] = pd.to_datetime(df.Date_Time , format = '%m/%d/%y %H:%M')
data = df.drop(['Date_Time'], axis=1)
data.index = df.Date_Time

cols = data.columns
for j in cols:
    for i in range(0,len(data)):
       if np.isnan(data[j][i]):
           data[j][i] = data[j][i-1]

#creating the train and validation set
train = data[:int(0.8*(len(data)))]
valid = data[int(0.8*(len(data))):]

#fit the model
from statsmodels.tsa.vector_ar.var_model import VAR

model = VAR(endog=train)
model_fit = model.fit()

# make prediction on validation
prediction = model_fit.forecast(model_fit.y, steps=len(valid))

#print(prediction)

#converting predictions to dataframe
pred = pd.DataFrame(index=range(0,len(prediction)),columns=[cols])
for j in range(0,6):
    for i in range(0, len(prediction)):
       pred.iloc[i][j] = prediction[i][j]

#check rmse
for i in cols:
    print('rmse value for', i, 'is : ', m.sqrt(mean_squared_error(pred[i], valid[i])))

model = VAR(endog=data)
model_fit = model.fit()
yhat = model_fit.forecast(model_fit.y, steps=20)
print(yhat)
