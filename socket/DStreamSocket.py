"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 This program uses Spark Structured Streaming Counts words in UTF8 encoded, '\n' delimited text received from the network every 10 second.
 Usage: DStreamSocket.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run
   `$ bin/spark-submit ../VARonStreaming/DatageneratorSocket.py localhost <port>`
 and then run
    `$ bin/spark-submit ../VARonStreaming/socket/StreamSocket.py localhost <port>`
"""
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pandas as pd
from CustomVAR import processStream
import sys

def parserecords(time, rdd):
    a1 = rdd.map(lambda w: w.split(","))
    a2 = [x for x in a1.toLocalIterator()]
    pandadf = pd.DataFrame(a2)

    if not pandadf.empty:
	    processStream(pandadf)


if len(sys.argv) != 3:
        print("Usage: DStreamSocket.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

# Create a local StreamingContext with two working thread and batch interval of 10 second
sc = SparkContext("local[2]", "TimeSeriesAnalytics")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
# lines.pprint()
# lines = lines.window(30, 10)
lines.foreachRDD(parserecords)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate