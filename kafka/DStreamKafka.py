"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 Generate Prediction using Vector Auto Regression (VAR) on input streams from Kafka brokers.
 Stream processing framwork in Apache Spark DiscreteStream (DStream)
 Usage: DStreamKafka.py <host:port> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      $SPARK_HOME/jars/spark-streaming-kafka-assembly-*.jar \
      ./VARonStreams/kafka/DStreamKafka.py \
      localhost:2181 test`
"""
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import findspark
findspark.init()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars $SPARK_HOME/jars/spark-streaming-kafka-assembly-*.jar pyspark-shell"
import sys
import pandas as pd
from CustomVAR import processStream

def parserecords(time, rdd):
    a1 = rdd.map(lambda w: w.split(","))
    a2 = [x for x in a1.toLocalIterator()]
    pandadf = pd.DataFrame(a2)
    pandadf.head(9)

    if not pandadf.empty:
	    processStream(pandadf)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: DStreamKafka.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    
    
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    lines = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # lines = lines.window(20,10) ## Working on stream windows
    lines.foreachRDD(parserecords)

    ssc.start()
    ssc.awaitTermination()