#!/usr/bin/python
# -*- coding: utf-8 -*-

import socketserver
import time

message = 'Hi I am server!'


class MyTCPHandler(socketserver.BaseRequestHandler):

    """
    The RequestHandler class for our server.
    
    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    print('Now I am here')
    def handle(self):
        print('Now inside the method')
        i = 0
        with open('Only-R80711-SC.csv', 'r') as fo:
            for line in fo:
                if i <= 10:
                    print(line)
                    self.request.sendall(bytes(line, 'utf-8'))
                    i = i + 1
                else:
                    i = 0
                    break #time.sleep(10)
                    socketserver.server_close()
        #print 'Done sending'
        

if __name__ == '__main__':
    (HOST, PORT) = ('localhost', 8887)

    # Create the server, binding to localhost on port 

    server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    
    server.handle_request()
    #server.server_close()
    #server.serve_forever()
