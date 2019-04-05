import socket
import time

HOST = 'localhost'        # The remote host
PORT = 42052              # The same port as used by the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
i = 0
with open("Only-R80711-SC.csv", "r") as fo:
        for line in fo:
		if i <= 100:
			print line
			#print i
			s.send(line)
			i = i + 1
		else:
			i = 0
			time.sleep(0)
#s.send("hi")
#s.shutdown(socket.SHUT_RDWR)
print "Done sending"
s.close()
