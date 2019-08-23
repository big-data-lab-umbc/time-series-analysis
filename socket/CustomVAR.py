from statsmodels.tsa.api import VAR
from random import seed

def processStream(df: object) -> object:
    print(df.shape)
    df.columns = ['datetime', 'src1', 'src2']
    data = df.drop(['datetime'], axis=1)
    data.index = df.datetime
    # print('date time indexing done!-------------------------------------')
    seed(1)
    cols = data.columns
    train = data[:int(0.8 * (len(data)))]
    valid = data[int(0.8 * (len(data))):]
    try:
        model = VAR(endog=train.astype(float),freq=None)
        model_fit = model.fit()
    # print(model_fit.summary())
        yhat = model_fit.forecast(model_fit.y,steps=1)
        print('***** Predicted Value *****')
        print(yhat)
    except ValueError:  # raised if `y` is empty.
        pass