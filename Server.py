import socket
from threading import Thread
import os, time
import json

def client_connection(ipc,ptc,send_data):
    ipc = str(ipc)
    ptc = int(ptc)
    client_request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_request.connect((ipc,ptc))
        client_request.send(send_data)
        data = client_request.recv(1024)
        client_request.close()
    except:
        data = "no connection possible"
    return data

def convert_to_string(data):
    ret = ""
    for i in data:
        ret += str(i)
        ret += " "
    ret = ret.strip()
    return ret
class Server(Thread):
    def __init__(self, ip, port,total_node, predecessor,M,finger_table):
        Thread.__init__(self)
        self.ip = str(ip)
        self.port = int(port)
        self.hb_ip = self.ip
        self.hb_port = int(random.randint(10000,11000))
        self.suc_hb_ip = None
        self.suc_hb_port = None
        self.total_node = int(total_node)
        self.socket_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_listen.bind((self.ip, self.port))
        self.socket_listen.listen(5)
        self.position = hash(self.ip+str(self.port)) % (self.total_node)
        self.finger_table = finger_table
        self.M = int(M)
        self.predecessor = predecessor
        self.key_table = {}
        print 'Started Node at port: ', self.port, "at position :", self.position

    def send_heart_beat(self):
        threading.Timer(5.0, self.send_heart_beat).start()
        client_connection(self.suc_hb_ip, self.suc_hb_port, "I am alive")
        return

    def listen_heart_beat(self):
        hb_listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        hb_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        hb_listen.bind((self.hb_ip, self.hb_port))
        hb_listen.listen(5)
        hb_listen.settimeout(8)
        while True:
            data = "dead"
            conn, addr = hb_listen.accept()
            data = conn.recv(1024)
            send_data = "ok"
            if data != "I am alive":
                repair_failure()
            else:
                conn.send(send_data)
            conn.close()
        self.socket_listen.close()

    def print_key_table(self):
        print "Key Table is "
        for key in self.key_table:
            print key, self.key_table[key][0], self.key_table[key][1]
        print

    def create_ring(self):
        self.predecessor = [self.position,self.ip,self.port]
        for i in range(self.M):
            self.finger_table[i] = [self.position,self.ip,self.port]

    def cpn(self,sid):
        for i in range(self.M-1,-1,-1):
            if self.belongTofunction(self.finger_table[i][0],self.position,sid,False):
                return i
        return -1

    def belongTofunction(self,sid,x,y,rt_include):
        sid = int(sid)
        x = int(x)
        y = int(y)
        if sid<=x:
            sid += self.total_node
        if y<=x:
            y += self.total_node

        if rt_include:
            if sid>x and sid<=y:
                return True
            else:
                return False
        else:
            if sid>x and sid<y:
                return True
            else:
                return False

    def find_succesor(self,sid):
        sid = int(sid)
        if sid == int(self.position):
            return str(sid)

        if self.belongTofunction(sid,self.position,self.finger_table[0][0],True):
            return convert_to_string(self.finger_table[0])
        else:
            nd = self.cpn(sid)
            send_data = "find_succesor "+str(sid)
            return client_connection(self.finger_table[nd][1],self.finger_table[nd][2],send_data)

    def join(self,mip,mport):
        mip = str(mip)
        mport = int(mport)
        self.predecessor = [0,0,0]
        for i in range(self.M):
            self.finger_table[i] = [self.position,self.ip,self.port]
        send_data = "find_succesor "+str(self.position)
        data = client_connection(mip,mport,send_data)
        data = data.strip()
        data = data.split()
        self.finger_table[0][0] = data[0]
        self.finger_table[0][1] = data[1]
        self.finger_table[0][2] = data[2]
        send_data = "update_your_predecessor "+convert_to_string([self.position,self.ip,self.port])
        data = client_connection(data[1],int(data[2]),send_data)
        data = data.strip()
        data = data.split()
        self.predecessor = data
        send_data = "update_your_successor " + convert_to_string([self.position,self.ip,self.port])
        data = client_connection(data[1],int(data[2]),send_data)
        self.update_finger(0,self.total_node-1,0,0,True)
        send_data = "ring_update "+str(self.predecessor[0])+" "+ str(self.position)+" "+str(self.ip)+" "+str(self.port)
        data = client_connection(self.finger_table[0][1],self.finger_table[0][2],send_data)
        send_data = "get_my_keys "+str(self.predecessor[0])
        data = client_connection(self.finger_table[0][1],self.finger_table[0][2],send_data)
        self.key_table = json.loads(data)
        send_data = "get_hb_info"
        data = client_connection(self.finger_table[0][1],self.finger_table[0][2],send_data)
        data = data.strip()
        data = data.split()
        self.suc_hb_ip = data[0]
        self.suc_hb_port = data[1]
    def update_finger(self,x,y,yip,yport,flag):
        x = int(x)
        y = int(y)
        for i in range(1,self.M):
            if flag:
                send_data = "find_succesor " + str((self.position+2**i)%self.total_node)
                data = client_connection(self.finger_table[0][1],self.finger_table[0][2],send_data)
                data = data.strip()
                data = data.split()
                self.finger_table[i] = data
            else:
                if self.belongTofunction((self.position+2**i)%self.total_node,x,y,True):
                    self.finger_table[i] = [y,yip,yport]

    def add_key(self,key):
        key_id = hash(key)%self.total_node
        print "actually where needs to add"+" "+str(key_id)
        key_ip = self.find_succesor(key_id)
        key_ip = key_ip.strip()
        key_ip = key_ip.split()
        send_data = "update_key_table "+key+" "+str(self.ip)+" "+str(self.port)
        data = client_connection(key_ip[1],key_ip[2],send_data)
        return "key added " + convert_to_string(key_ip) + " with key_id " + str(key_id)

    def update_key_table(self,key,iip,iport):
        self.key_table[key] = (iip,iport)

    def find_key(self, key):
        key_id = hash(key)%self.total_node
        key_ip = self.find_succesor(key_id)
        key_ip = key_ip.strip()
        key_ip = key_ip.split()
        send_data = "retrieve_key " + key
        data = client_connection(key_ip[1], key_ip[2], send_data)
        return data

    def retrieve_key(self, key):
        return str(self.key_table[key][0]) + " " + str(self.key_table[key][1])

    def others_key_entry(self,l,r):
        send_dic = {}
        ac_dic = {}
        for key in self.key_table:
            key_id = hash(key)%self.total_node
            if self.belongTofunction(key_id,l,r,True):
                send_dic[key] = self.key_table[key]
            else:
                ac_dic[key] = self.key_table[key]
        self.key_table = ac_dic
        return send_dic
    def run(self):
        self.send_heart_beat()
        thread.start_new_thread(self.listen_heart_beat, ())
        while True:
            conn, addr = self.socket_listen.accept()
            data = conn.recv(1024)
            data = data.strip()
            data = data.split()
            if data[0]=="find_succesor":
                send_data = self.find_succesor(int(data[1]))
                conn.send(send_data)

            elif data[0]=="update_your_predecessor":
                send_data = convert_to_string(self.predecessor)
                self.predecessor[0] = data[1]
                self.predecessor[1] = data[2]
                self.predecessor[2] = data[3]
                conn.send(send_data)

            elif data[0]=="update_your_successor":
                self.finger_table[0][0] = data[1]
                self.finger_table[0][1] = data[2]
                self.finger_table[0][2] = data[3]
                send_data = "ok"
                conn.send(send_data)

            elif data[0]=="ring_update":
                if int(data[2])!=self.position:
                    self.update_finger(data[1],data[2],data[3],data[4],False)
                    send_data = "ring_update "+data[1]+" "+data[2]+" "+data[3]+" "+data[4]
                    data = client_connection(self.finger_table[0][1],self.finger_table[0][2],send_data)
                send_data = "ok"
                conn.send(send_data)

            elif data[0]=="update_key_table":
                self.update_key_table(data[1],data[2],data[3])

            elif data[0]=="retrieve_key":
                to_send = self.retrieve_key(data[1])
                conn.send(to_send)

            elif data[0]=="get_my_keys":
                send_dic = self.others_key_entry(data[1],self.predecessor[0])
                send_data = json.dumps(send_dic)
                conn.send(send_data)

            elif data[0]=="get_hb_info":
                send_data = str(self.hb_ip)+" "+str(self.hp_port)
                conn.send(send_data)

            elif data[0]=="close":
                break
            conn.close()
        self.socket_listen.close()
