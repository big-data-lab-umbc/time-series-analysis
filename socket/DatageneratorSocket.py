"""
 Author: Arjun Pandya, Department of Information Systems UMBC, BigDataLabs
 Date: 08/21/2019

 This program uses Spark Structured Streaming Counts words in UTF8 encoded, '\n' delimited text received from the network every 10 second.
 Usage: DatageneratorSocket.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run
   `$ bin/spark-submit ../VARonStreaming/socket/DatageneratorSocket.py localhost <port>`

"""
from time import sleep
import numpy as np
import math
import datetime as dt
import socketserver
import sys

class MyTCPHandler(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    print('Data generator is ready. Waiting for Connections...')

    def handle(self):
        print('Streaming...')
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
            if i <= 10:

                source1[x] = 0.95 * math.sqrt(2) * source1[x - 1] - 0.90 * source1[x - 2] + noise[x]
                source2[x] = 0.5*source2[x-2] + noise2[x]
                msg = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ',' + str(source1[x]) + ',' + str(source2[x]) + "\n"
                # print(msg)
                self.request.sendall(bytes(msg, 'utf-8'))
                i += 1
            else:
                i = 0
                sleep(1)

if __name__ == '__main__':
    # if len(sys.argv) != 3:
    #     print("Usage: DStreamSocket.py <hostname> <port>", file=sys.stderr)
    #     sys.exit(-1)

    # (HOST, PORT) = (sys.argv[1], int(sys.argv[2]))
    (HOST, PORT) = ('localhost', 8885)
    # Create the server, binding to localhost on port
    server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.handle_request()