import socket

HOST = 'localhost'         # server ip
PORT = 42052              # Arbitrary non-privileged port
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
print "Server running", HOST, PORT
s.listen(5)
conn, addr = s.accept()
print'Connected by', addr

f = open("tsa_source.csv","a+")

#l = "".join(iter(lambda:conn.recv(1),"\n")) 

while True:
	data = "".join(iter(lambda:conn.recv(1),"\n"))       
	print data
 	if data == "": 
		#print "I am here"
		break 
	f.write(data)          
      
print "Done Receiving"
f.close()
conn.close()



