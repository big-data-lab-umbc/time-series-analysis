import socket

HOST = 'localhost'         # server ip
PORT = 42050              # Arbitrary non-privileged port
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
print "Server running", HOST, PORT
s.listen(5)
conn, addr = s.accept()
print'Connected by', addr

while True:
    data = "".join(iter(lambda:conn.recv(1),"\n"))       
    print data   
    if not data: 
		break                
      
print "Done Receiving"
conn.close()