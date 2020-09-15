from pyspark.sql import SparkSession
# from pyspark.sql.functions import l
import pickle
from statsmodels.tsa.api import VAR
from random import seed
from datetime import datetime


spark = SparkSession \
.builder \
.appName("Batch Process") \
.enableHiveSupport() \
.getOrCreate()

df = spark.sql("select * FROM default.turbine order by ts desc")
pdf = df.toPandas()
pdf.columns = ['ts', 'rt_temp', 'st_temp']
data = pdf.drop(['ts'], axis=1)
data.index = pdf.ts
model = VAR(endog=data.astype(float))
currentDT = datetime.now()
path = "/afs/umbc.edu/users/a/p/apandya1/home/TSA/VARonStreams/kafka/VAR"+ currentDT.strftime("%Y-%m-%d%H:%M:%S") +".pkl"
modelpick = open(path, "wb")
pickle.dump(model, modelpick)
modelpick.close()
