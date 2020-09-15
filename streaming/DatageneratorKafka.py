"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 Generate Prediction using Vector Auto Regression (VAR) on input streams from Kafka brokers.
 Stream processing framwork in Apache Spark DiscreteStream (DStream)
 Usage: SStreamKafka.py <host:port> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart or run DatageneratorKafka.py
 and then run the code
    
    `$ bin/spark-submit ./VARonStreams/kafka/DatageneratorKafka.py localhost:2181 test`
"""
from time import sleep
from json import dumps
from kafka import KafkaProducer
import numpy as np
import math
import datetime as dt
import sys
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: DatageneratorKafka.py <hostname:port> <topic>", file=sys.stderr)
        sys.exit(-1)
    broker = sys.argv[1]
    topic = sys.argv[2]
    k = 100000
    np.set_printoptions(suppress=True)
    noise = np.random.normal(0, 1, k)
    noise2 = np.random.normal(0, 1, k)
    source1 = np.zeros((k))
    source1[1] = noise[1] + 10
    source1[2] = noise[2] + 10
    source2 = np.zeros((k))
    source2[1] = noise2[1]
    source2[2] = noise2[2]
    i = 0
    for x in range(3, k):
        source1[x] = 0.95 * math.sqrt(2) * source1[x - 1] - 0.90 * source1[x - 2] + noise[x]
        source2[x] = 0.5*source2[x-2] + noise2[x]
        msg = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ',' + str(source1[x]) + ',' + str(source2[x])
        # print(msg)
        producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
        producer.send(topic, value=msg)
        sleep(1)