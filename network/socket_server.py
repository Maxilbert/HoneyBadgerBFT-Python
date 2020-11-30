import time
import pickle
from typing import List

import gevent
import os

from multiprocessing import Value as mpValue, Queue as mpQueue, Process, Semaphore as mpSemaphore
from gevent import socket, monkey, lock

import logging
import traceback

#monkey.patch_all(thread=False, socket=False)
monkey.patch_all(thread=False)



# Network node class: deal with socket communications
class NetworkServer (Process):

    SEP = '\r\nSEP\r\nSEP\r\nSEP\r\n'

    def __init__(self, port: int, my_ip: str, id: int, addresses_list: list, recv_q: mpQueue, server_ready: mpValue, stop: mpValue):

        self.recv_queue = recv_q
        self.ready = server_ready
        self.stop = stop

        self.ip = my_ip
        self.port = port
        self.id = id
        self.addresses_list = addresses_list
        self.N = len(self.addresses_list)

        self.is_in_sock_connected = [False] * self.N

        self.socks = [None for _ in self.addresses_list]
        self.sock_locks = [lock.Semaphore() for _ in self.addresses_list]

        super().__init__()

    def _handle_ingoing_msg(self, sock, jid):

        buf = b''
        try:
            while not self.stop.value:
                gevent.sleep(0)
                time.sleep(0)
                buf += sock.recv(5000)
                tmp = buf.split(self.SEP.encode('utf-8'), 1)
                while len(tmp) == 2:
                    buf = tmp[1]
                    data = tmp[0]
                    if data != '' and data:
                        if data == 'ping'.encode('utf-8'):
                            sock.sendall('pong'.encode('utf-8'))
                            self.logger.info("node {} is pinging node {}...".format(jid, self.id))
                            self.is_in_sock_connected[jid] = True
                            if all(self.is_in_sock_connected):
                                with self.ready.get_lock():
                                    self.ready.value = True
                        else:
                            (j, o) = (jid, pickle.loads(data))
                            assert j in range(self.N)
                            self.recv_queue.put_nowait((j, o))
                            #self.logger.info('recv' + str((j, o)))
                            #print('recv' + str((j, o)))
                    else:
                        self.logger.error('syntax error messages')
                        raise ValueError
                    tmp = buf.split(self.SEP.encode('utf-8'), 1)
                gevent.sleep(0)
                time.sleep(0)
        except Exception as e:
            self.logger.error(str((e, traceback.print_exc())))

    def _listen_and_recv_forever(self):
        pid = os.getpid()
        self.logger.info('node %d\'s socket server starts to listen ingoing connections on process id %d' % (self.id, pid))
        print("my IP is " + self.ip)
        self.server_sock = socket.socket()
        self.server_sock.bind((self.ip, self.port))
        self.server_sock.listen(5)
        handle_msg_threads = []
        while not self.stop.value:
            gevent.sleep(0)
            time.sleep(0)
            sock, address = self.server_sock.accept()
            jid = self._address_to_id(address)
            msg_t = gevent.spawn(self._handle_ingoing_msg, sock, jid)
            handle_msg_threads.append(msg_t)
            self.logger.info('node id %d accepts a new socket from node %d' % (self.id, self._address_to_id(address)))
            gevent.sleep(0)
            time.sleep(0)
        #gevent.joinall(handle_msg_threads)
        #gevent.sleep(5)
        #time.sleep(5)



    def run(self):
        pid = os.getpid()
        self.logger = self._set_server_logger(self.id)

        self.logger.info('node id %d is running on pid %d' % (self.id, pid))
        with self.ready.get_lock():
            self.ready.value = False

        gevent.spawn(self._listen_and_recv_forever).join()


    def _address_to_id(self, address: tuple):
        for i in range(self.N):
            if address[0] != '127.0.0.1' and address[0] == self.addresses_list[i][0]:
                return i
        return int((address[1] - 10000) / 200)

    def _set_server_logger(self, id: int):
        logger = logging.getLogger("node-" + str(id))
        logger.setLevel(logging.DEBUG)
        # logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
        if 'log' not in os.listdir(os.getcwd()):
            os.mkdir(os.getcwd() + '/log')
        full_path = os.path.realpath(os.getcwd()) + '/log/' + "node-net-server-" + str(id) + ".log"
        file_handler = logging.FileHandler(full_path)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        logger.addHandler(file_handler)
        return logger
