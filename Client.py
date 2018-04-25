import socket
import os
import socket
from threading import Thread
import os, time


class Client(object):
    def __init__(self, total_node):
        self.total_node = total_node
        # print 'Started Node at port: ', self.port, "at position :", self.position

    def run(self,ipc,ptc,send_data):
        client_request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_request.connect((ipc,ptc))
        client_request.send(send_data)
        data = client_request.recv(1024)
        client_request.close()
        return data
if __name__ == '__main__':
    clc = Client(256)
    clc.run('localhost',9001,"close")
