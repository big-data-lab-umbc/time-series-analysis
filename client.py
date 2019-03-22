import socket
import time

HOST = 'localhost'        # The remote host
PORT = 42050              # The same port as used by the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
f = open('my.csv', 'rb')
print "Sending Data ...."  
i = 0
l = f.read()
while True:      
    for line in l:
        s.send(line)
    break
f.close()
print "Sending Complete"
s.close()