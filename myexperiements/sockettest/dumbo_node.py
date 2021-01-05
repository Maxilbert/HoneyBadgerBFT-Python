import random
from typing import Callable, List
import os
import pickle
from gevent.event import Event
from gevent.queue import Queue

from dumbobft.core.dumbo import Dumbo
from myexperiements.sockettest.make_random_tx import tx_generator


def load_key(id, N):

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'sPK.key', 'rb') as fp:
        sPK = pickle.load(fp)

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'sPK1.key', 'rb') as fp:
        sPK1 = pickle.load(fp)

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'ePK.key', 'rb') as fp:
        ePK = pickle.load(fp)

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'sSK-' + str(id) + '.key', 'rb') as fp:
        sSK = pickle.load(fp)

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'sSK1-' + str(id) + '.key', 'rb') as fp:
        sSK1 = pickle.load(fp)

    with open(os.getcwd() + '/keys-' + str(N) + '/' + 'eSK-' + str(id) + '.key', 'rb') as fp:
        eSK = pickle.load(fp)

    return sPK, sPK1, ePK, sSK, sSK1, eSK


class DumboBFTNode (Dumbo):

    def __init__(self, sid, id, B, N, f, recv_queue: Queue, send_queues: List[Queue], net_ready: Event, stop: Event, K=3, mode='debug', mute=False, tx_buffer=None):
        self.sPK, self.sPK1, self.ePK, self.sSK, self.sSK1, self.eSK = load_key(id, N)

        def make_send():
            def send(j, o):
                if j == -1:
                    for _ in range(N):
                        send_queues[_].put_nowait(o)
                else:
                    send_queues[j].put_nowait(o)
            return send

        self.send = make_send()
        self.recv = lambda: recv_queue.get()
        self.ready = net_ready
        self.stop = stop
        self.mode = mode
        Dumbo.__init__(self, sid, id, max(int(B/N), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.ePK, self.eSK, self.send, self.recv, K=K, mute=mute)

    def prepare_bootstrap(self):
        self.logger.info('node id %d is inserting dummy payload TXs' % (self.id))
        if self.mode == 'test' or 'debug': #K * max(Bfast * S, Bacs)
            tx = tx_generator(250)  # Set each dummy TX to be 250 Byte
            k = 0
            for _ in range(self.K):
                for r in range(self.B):
                    Dumbo.submit_tx(self, tx.replace(">", hex(r) + ">"))
                    k += 1
                    if r % 50000 == 0:
                        self.logger.info('node id %d just inserts 50000 TXs' % (self.id))
        else:
            pass
            # TODO: submit transactions through tx_buffer
        self.logger.info('node id %d completed the loading of dummy TXs' % (self.id))

    def run(self):

        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))

        self._send = self.send
        self._recv = self.recv

        self.prepare_bootstrap()

        self.ready.wait()

        self.run_bft()
        self.stop.set()

def main(sid, i, B, N, f, addresses, K):
    badger = DumboBFTNode(sid, i, B, N, f, addresses, K)
    badger.run_bft()


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
    args = parser.parse_args()

    # Some parameters
    sid = args.sid
    i = args.id
    N = args.N
    f = args.f
    B = args.B
    K = args.K

    # Random generator
    rnd = random.Random(sid)

    # Nodes list
    host = "127.0.0.1"
    port_base = int(rnd.random() * 5 + 1) * 10000
    addresses = [(host, port_base + 200 * i) for i in range(N)]
    print(addresses)

    main(sid, i, B, N, f, addresses, K)
