from statsmodels.tsa.api import VAR
from random import seed
from datetime import datetime

def processStream(df: object,streamtype: str) -> object:

    df.columns = ['datetime', 'rt_temp', 'st_temp']
    data = df.drop(['datetime'], axis=1)
    data.index = df.datetime
    # print('date time indexing done!-------------------------------------')
    seed(1)
    cols = data.columns
    train = data[:int(0.8 * (len(data)))]
    valid = data[int(0.8 * (len(data))):]
    try:
        print(df.shape)
        model = VAR(endog=train.astype(float),freq=None)
        model_fit = model.fit()
    # print(model_fit.summary())
        yhat = model_fit.forecast(model_fit.y,steps=1)
        # print('***** Predicted Value *****')
        currentDT = datetime.now()
        pred = str(yhat) + ',' + currentDT.strftime("%Y-%m-%d%H:%M:%S") + '\n'
        pred = pred.replace('[[','').replace(']]','').replace(' ', ',').replace(',,', ',').lstrip(',')
        print(pred)
        # print(pred.replace('[[','').replace(']]','').replace(' ',','))
	#currentDT = datetime.now()
        results_fileName = streamtype +'_Predictions_at_' + currentDT.strftime("%Y-%m-%d")
        # f = open("/Users/arjunpandya/PycharmProjects/VARonStreams/TSA-Predictions/%s.csv" %results_fileName, "a+")
        f = open("/afs/umbc.edu/users/a/p/apandya1/home/TSA/VARonStreams/TSA-Predictions/%s.txt" %results_fileName, "a+")
        f.write(pred)
        f.close()
    except ValueError:  # raised if `y` is empty.
        pass