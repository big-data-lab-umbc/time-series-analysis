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
import numpy as np
import math
import datetime as dt
import sys
if __name__ == '__main__':
    f = open('../source/turbine_actual.csv', "+r")
    out_str = []
    lines = f.readlines()
    j = 0
    for line in lines:
        x1, x2 = line.split(",")

        # print()
        for i in range (1,4):
            x1 = round(0.1 * 1 + float(x1),2)
            x2 = round(0.1 * 1 + float(x2),2)
            # print('x1', x1)
            # print('x2', x2)
            # print(j)
            out_str.append(str(x1) +','+ str(x2))
    onp = np.array(out_str)
    np.savetxt('../source/gradual.csv', onp, delimiter=",", fmt="%s")