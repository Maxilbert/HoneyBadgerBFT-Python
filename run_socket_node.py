import time
import random
import traceback
import gevent

from typing import List

from gevent import monkey
monkey.patch_all(thread=False)

from myexperiements.sockettest.dumbo_node import DumboBFTNode
from myexperiements.sockettest.dumbox_node import DumboXBFTNode
from myexperiements.sockettest.mule_node import MuleBFTNode
from myexperiements.sockettest.socket_server import NetworkServer

from multiprocessing import Value as mpValue, Queue as mpQueue
from ctypes import c_bool

def instantiate_bft_node(sid, i, B, N, f, K, S, T, recv_q: mpQueue, send_q: List[mpQueue], ready: mpValue, stop: mpValue, protocol="mule", mute=False, factor=1):
    bft = None
    if protocol == 'dumbo':
        bft = DumboBFTNode(sid, i, B, N, f, recv_q, send_q, ready, stop, K, mute=mute)
    elif protocol == 'dumbox':
        bft = DumboXBFTNode(sid, i, B, N, f, recv_q, send_q, ready, stop, K, mute=mute)
    elif protocol == "mule":
        bft = MuleBFTNode(sid, i, S, T, int(factor*B), B, N, f, recv_q, send_q, ready, stop, K, mute=mute)
    else:
        print("Only support dumbo or dumbox or mule")
    return bft



if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sid', metavar='sid', required=True,
                        help='identifier of node', type=str)
    parser.add_argument('--id', metavar='id', required=True,
                        help='identifier of node', type=int)
    parser.add_argument('--N', metavar='N', required=True,
                        help='number of parties', type=int)
    parser.add_argument('--f', metavar='f', required=True,
                        help='number of faulties', type=int)
    parser.add_argument('--B', metavar='B', required=True,
                        help='size of batch', type=int)
    parser.add_argument('--K', metavar='K', required=True,
                        help='rounds to execute', type=int)
    parser.add_argument('--S', metavar='S', required=False,
                        help='slots to execute', type=int, default=50)
    parser.add_argument('--T', metavar='T', required=False,
                        help='fast path timeout', type=float, default=1)
    parser.add_argument('--P', metavar='P', required=False,
                        help='protocol to execute', type=str, default="mule")
    parser.add_argument('--M', metavar='M', required=False,
                        help='whether to mute a third of nodes', type=bool, default=False)
    parser.add_argument('--F', metavar='F', required=False,
                        help='the parameter to time (B/N) to get the fast path batch size', type=float, default=1)

    args = parser.parse_args()

    # Some parameters
    sid = args.sid
    i = args.id
    N = args.N
    f = args.f
    B = args.B
    K = args.K
    S = args.S
    T = args.T
    P = args.P
    M = args.M
    F = args.F

    # Random generator
    rnd = random.Random(sid)

    # Nodes list
    addresses = [None] * N
    try:
        with open('hosts.config', 'r') as hosts:
            for line in hosts:
                params = line.split()
                pid = int(params[0])
                priv_ip = params[1]
                pub_ip = params[2]
                port = int(params[3])
                # print(pid, ip, port)
                if pid not in range(N):
                    continue
                if pid == i:
                    my_address = (priv_ip, port)
                addresses[pid] = (pub_ip, port)
        assert all([node is not None for node in addresses])
        print("hosts.config is correctly read")

        recv_q = mpQueue()
        send_q = [mpQueue() for _ in range(N)]
        ready = mpValue(c_bool, False)
        stop = mpValue(c_bool, False)

        #net_server = NetworkServer(my_address[1], my_address[0], i, addresses, recv_q, server_ready, stop)
        #net_client = NetworkClient(my_address[1], my_address[0], i, addresses, send_q, client_ready, stop)
        bft = instantiate_bft_node(sid, i, B, N, f, K, S, T, recv_q, send_q, ready, stop, P, M, F)
        net = NetworkServer(my_address[1], my_address[0], i, addresses, recv_q, send_q, ready, stop)

        net.start()

        while not ready.value:
            time.sleep(1)
            gevent.sleep(1)
            print("waiting for network ready...")

        bft.run()


    except FileNotFoundError or AssertionError as e:
        traceback.print_exc()

