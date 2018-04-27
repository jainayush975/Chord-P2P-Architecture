import socket
import os
import sys
from Server import Server,client_connection



ip = raw_input("Ip of connecting Node: ")
ip = str(ip)
port = raw_input("Type port to use: ")
port = int(port)
mBit = raw_input("Power of size in your ring: ")
mBit = int(mBit)
total_node = 2**mBit
threads = []
finger = [-1 for i in range(mBit)]
predecessor = None

active_server = Server(ip, port,total_node, predecessor,mBit,finger)
active_server.start()
threads.append(active_server)

while True:
	x = raw_input("chord>> ")
	if x=="":
		continue
	x = x.strip()
	x = x.split()
	if x[0]=="print":
		print ip,port,active_server.predecessor
		print active_server.finger_table
	elif x[0]=="create_ring":
		active_server.create_ring()
		print "Ring created with only node in it",active_server.position

	elif x[0]=="join":
		active_server.joinNode(x[1],x[2])
		print "Node joined succesfully"

	elif x[0]=="find":
		print active_server.find_succesor(x[1])

	elif x[0]=="add_key":
		print active_server.add_key(x[1])

	elif x[0]=="find_key":
		print "Required key is present on ", active_server.find_key(x[1])

	elif x[0]=="print_table":
		active_server.print_key_table()

	elif x[0]=="close":
		active_server.take_my_keys()
		active_server.dead = True
		client_connection(ip,port,"close")
		# quit()
		break
	elif x[0]=="print_hb":
		print active_server.hb_ip
		print active_server.hb_port
		print active_server.suc_hb_ip
		print active_server.suc_hb_port
	# active_server.join()
