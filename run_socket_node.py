import sys
if 'threading' in sys.modules:
    raise Exception('threading module loaded before patching!')
from gevent import Greenlet, monkey; monkey.patch_all(thread=False)

import time
import random
import traceback
from typing import List, Callable
from gevent.event import Event
from gevent.queue import Queue
from myexperiements.sockettest.dumbo_node import DumboBFTNode
from myexperiements.sockettest.mule_node import MuleBFTNode
from myexperiements.sockettest.hotstuff_node import HotstuffBFTNode
from network.socket_server import NetworkServer
from network.socket_client import NetworkClient


def instantiate_bft_node(sid, i, B, N, f, K, S, T, recv_queue: Queue, send_queues: List[Queue], net_ready: Event,
                         stop: Event, protocol="mule", mute=False, F=100000):
    bft = None
    if protocol == 'dumbo':
        bft = DumboBFTNode(sid, i, B, N, f, recv_queue, send_queues, net_ready, stop, K, mute=mute)
    elif protocol == "mule":
        bft = MuleBFTNode(sid, i, S, T, B, F, N, f, recv_queue, send_queues, net_ready, stop, K, mute=mute)
    elif protocol == 'hotstuff':
        bft = HotstuffBFTNode(sid, i, S, T, B, F, N, f, recv_queue, send_queues, net_ready, stop, 1, mute=mute)
    else:
        print("Only support dumbo or dumbox or mule or hotstuff")
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
                        help='batch size of fallback path', type=int, default=100000)

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

        # bft_from_server, server_to_bft = mpPipe(duplex=True)
        # client_from_bft, bft_to_client = mpPipe(duplex=True)

        send_queues = [Queue() for _ in range(N)]

        recv_queue = Queue()

        client_ready = Event()
        client_ready.clear()
        server_ready = Event()
        server_ready.clear()
        net_ready = Event()
        net_ready.clear()
        stop = Event()
        stop.clear()

        net_server = NetworkServer(my_address[1], my_address[0], i, addresses, recv_queue, server_ready, stop)
        net_client = NetworkClient(my_address[1], my_address[0], i, addresses, send_queues, client_ready, stop)
        bft = instantiate_bft_node(sid, i, B, N, f, K, S, T, recv_queue, send_queues, net_ready, stop, P, M, F)

        net_server.start()
        net_client.start()

        while not client_ready.is_set() and not server_ready.is_set:
            time.sleep(1)
            print("waiting for network ready...")

        net_ready.set()

        bft_thread = Greenlet(bft.run)
        bft_thread.start()
        bft_thread.join()
        #bft.run()

        stop.set()

        #net_client.stop_service()
        net_client.kill(timeout=1)
        net_client.join()
        time.sleep(1)
        #net_server.stop_service()
        net_client.kill(timeout=1)
        net_server.join()


    except FileNotFoundError or AssertionError as e:
        traceback.print_exc()
