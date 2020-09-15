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
import datetime as dt
import socketserver
import sys


class requesthandle(socketserver.BaseRequestHandler):
    """
    Request Handler Server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    print('Data transmitter is ready. Waiting for Connections...')

    def handle(self):
        print('Streaming...')
        f = open("../source/autoregsrc.csv", "+r")
        lines = f.readlines()
        # print(lines)
        for line in lines:
            msg = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ',' + line
            # print(msg)
            self.request.sendall(bytes(msg, 'utf-8'))
            # sleep(1)

if __name__ == '__main__':

    # if len(sys.argv) != 3:
    #     print("Usage: DStreamSocket.py <hostname> <port>", file=sys.stderr)
    #     sys.exit(-1)
    # (HOST, PORT) = (sys.argv[1], int(sys.argv[2]))
    (HOST, PORT) = ('localhost', 8899)
    # Create the server, binding to localhost on port
    server = socketserver.TCPServer((HOST, PORT), requesthandle)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.handle_request()
