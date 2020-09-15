"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2020

 Generate Prediction using Vector Auto Regression (VAR) on input streams from Kafka brokers.
 Stream processing framwork in Apache Spark DiscreteStream (DStream)
 Usage: SStreamKafka.py <host:port> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart or run DatageneratorKafka.py
 and then run the code
    
    `$ bin/spark-submit streamsourcedata.py localhost:2181 test './temp.csv'`
"""
from time import sleep
from json import dumps
from kafka import KafkaProducer
import numpy as np
import math
import datetime as dt
import sys
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: streamsourcedata.py <hostname:port> <topic>", file=sys.stderr)
        sys.exit(-1)
    broker = sys.argv[1]
    topic = sys.argv[2]
    src_file = str(sys.argv[3])
    f = open(src_file,"+r")
    lines = f.readlines()
    for line in lines:
        msg = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ',' + line
        producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
        producer.send(topic, value=msg)
        sleep(1)
