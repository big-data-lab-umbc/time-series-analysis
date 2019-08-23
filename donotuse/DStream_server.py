from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from test import printDF
import pandas as pd
import VecAutoReg as v

# Create a local StreamingContext with two working thread and batch interval of 10 second
sc = SparkContext("local[2]", "TrubineDataAnalytics")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost", 8887)

lines = lines.window(30, 10)

#data = lines.flatMap(lambda line: line.split(","))

def printRecord(time, rdd):
	
	a1 = rdd.map(lambda w: w.split(","))
	a2=[x for x in a1.toLocalIterator()]
	pandadf=pd.DataFrame(a2)
	#new_header = pandadf.iloc[0]
	#pandadf = pandadf[1:]
	#pandadf.columns = new_header
	print(pandadf)
	#pandadf.to_csv('panda_test_file.csv', index=False)
	if not pandadf.empty:
		v.process(pandadf)

lines.foreachRDD(printRecord)



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate