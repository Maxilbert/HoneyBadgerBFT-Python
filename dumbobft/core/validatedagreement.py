from gevent import Greenlet, monkey; monkey.patch_all()

import copy
import time
import traceback
import logging
from datetime import datetime

import gevent
import numpy as np
from collections import namedtuple

from gevent import monkey, Greenlet
from gevent.event import Event
from enum import Enum
from collections import defaultdict
from gevent.queue import Queue
from honeybadgerbft.core.commoncoin import shared_coin
from dumbobft.core.baisedbinaryagreement import baisedbinaryagreement
from dumbobft.core.consistentbroadcast import consistentbroadcast
from honeybadgerbft.exceptions import UnknownTagError
from crypto.threshsig.boldyreva import serialize, deserialize1



class MessageTag(Enum):
    VABA_COIN = 'VABA_COIN'             # Queue()
    VABA_COMMIT = 'VABA_COMMIT'         # [Queue()] * N
    VABA_VOTE = 'VABA_VOTE'             # [Queue()] * Number_of_ABA_Iterations
    VABA_ABA_COIN = 'VABA_ABA_COIN'     # [Queue()] * Number_of_ABA_Iterations
    VABA_CBC = 'VABA_CBC'               # [Queue()] * N
    VABA_ABA = 'VABA_ABA'               # [Queue()] * Number_of_ABA_Iterations


MessageReceiverQueues = namedtuple(
    'MessageReceiverQueues', ('VABA_COIN', 'VABA_COMMIT', 'VABA_VOTE', 'VABA_ABA_COIN', 'VABA_CBC', 'VABA_ABA'))


def recv_loop(recv_func, recv_queues):
    while True:
        #gevent.sleep(0.0001)
        sender, (tag, j, msg) = recv_func()
        #print("recv2", (sender, (tag, j, msg)))

        if tag not in MessageTag.__members__:
            raise UnknownTagError('Unknown tag: {}! Must be one of {}.'.format(
                tag, MessageTag.__members__.keys()))
        recv_queue = recv_queues._asdict()[tag]

        if tag not in {MessageTag.VABA_COIN.value}:
            recv_queue = recv_queue[j]
        try:
            recv_queue.put_nowait((sender, msg))
        except AttributeError as e:
            # print((sender, msg))
            traceback.print_exc(e)




def validatedagreement(sid, pid, N, f, PK, SK, PK1, SK1, input, decide, receive, send, predicate=lambda x: True, logger=None):
    """Multi-valued Byzantine consensus. It takes an input ``vi`` and will
    finally writes the decided value into ``decide`` channel.

    :param sid: session identifier
    :param pid: my id number
    :param N: the number of parties
    :param f: the number of byzantine parties
    :param PK: ``boldyreva.TBLSPublicKey`` with threshold f+1
    :param SK: ``boldyreva.TBLSPrivateKey`` with threshold f+1
    :param PK1: ``boldyreva.TBLSPublicKey`` with threshold n-f
    :param SK1: ``boldyreva.TBLSPrivateKey`` with threshold n-f
    :param input: ``input()`` is called to receive an input
    :param decide: ``decide()`` is eventually called
    :param receive: receive channel
    :param send: send channel
    :param predicate: ``predicate()`` represents the externally validated condition
    """

    #print("Starts to run validated agreement...")

    assert PK.k == f+1
    assert PK.l == N
    assert PK1.k == N-f
    assert PK1.l == N

    """ 
    """
    """ 
    Some instantiations
    """
    """ 
    """

    my_cbc_input = Queue(1)
    my_commit_input = Queue(1)
    aba_inputs = defaultdict(lambda: Queue(1))

    aba_recvs = defaultdict(lambda: Queue())
    aba_coin_recvs = defaultdict(lambda: Queue())
    vote_recvs = defaultdict(lambda: Queue())

    cbc_recvs = [Queue() for _ in range(N)]
    coin_recv = Queue()
    commit_recvs = [Queue() for _ in range(N)]

    cbc_outputs = [Queue(1) for _ in range(N)]
    commit_outputs = [Queue(1) for _ in range(N)]
    aba_outputs = defaultdict(lambda: Queue(1))

    is_cbc_delivered = [0] * N
    is_commit_delivered = [0] * N

    recv_queues = MessageReceiverQueues(
        VABA_COIN=coin_recv,
        VABA_COMMIT=commit_recvs,
        VABA_VOTE=vote_recvs,
        VABA_ABA_COIN=aba_coin_recvs,
        VABA_CBC=cbc_recvs,
        VABA_ABA=aba_recvs,
    )
    recv_loop_thred = Greenlet(recv_loop, receive, recv_queues)
    recv_loop_thred.start()

    """ 
    Setup the sub protocols Input Broadcast CBCs"""

    for j in range(N):

        def make_cbc_send(j): # this make will automatically deep copy the enclosed send func
            def cbc_send(k, o):
                """CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                #print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_CBC', j, o))
            return cbc_send

        # Only leader gets input
        cbc_input = my_cbc_input.get if j == pid else None
        cbc = gevent.spawn(consistentbroadcast, sid + 'CBC' + str(j), pid, N, f, PK1, SK1, j,
                           cbc_input, cbc_recvs[j].get, make_cbc_send(j), logger)
        # cbc.get is a blocking function to get cbc output
        cbc_outputs[j] = cbc.get

    """ 
    Setup the sub protocols Commit CBCs"""

    for j in range(N):

        def make_commit_send(j): # this make will automatically deep copy the enclosed send func
            def commit_send(k, o):
                """COMMIT-CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                #print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_COMMIT', j, o))
            return commit_send

        # Only leader gets input
        commit_input = my_commit_input.get if j == pid else None
        commit = gevent.spawn(consistentbroadcast, sid + 'COMMIT-CBC' + str(j), pid, N, f, PK1, SK1, j,
                           commit_input, commit_recvs[j].get, make_commit_send(j), logger)
        # commit.get is a blocking function to get commit-cbc output
        commit_outputs[j] = commit.get

    """ 
    Setup the sub protocols permutation coins"""

    def coin_bcast(o):
        """Common coin multicast operation.
        :param o: Value to multicast.
        """
        send(-1, ('VABA_COIN', 'leader_election', o))

    permutation_coin = shared_coin(sid + 'COIN', pid, N, f,
                               PK, SK, coin_bcast, coin_recv.get, False, logger)
    # False means to get a coin of 256 bits instead of a single bit

    """ 
    """
    """ 
    Start to run consensus
    """
    """ 
    """

    """ 
    Run n CBC instance to consistently broadcast input values
    """

    cbc_values = [None] * N

    def wait_for_input():
        v = input()
        if logger != None:
            logger.info("VABA %s get input at %s" % (sid, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
        #assert predicate(v)
        my_cbc_input.put_nowait(v)

    gevent.spawn(wait_for_input)

    wait_cbc_signal = Event()
    wait_cbc_signal.clear()

    def wait_for_cbc_to_continue(leader):
        # Receive output from CBC broadcast for input values
        msg, raw_Sigma = cbc_outputs[leader]()
        if predicate(msg):
            cbc_values[leader] = (msg, raw_Sigma)  # May block
            is_cbc_delivered[leader] = 1
            if sum(is_cbc_delivered) >= N - f:
                wait_cbc_signal.set()
            #print("Node %d finishes CBC for Leader %d" % (pid, leader) )
            #print(is_cbc_delivered)

    cbc_out_threads = [gevent.spawn(wait_for_cbc_to_continue, node) for node in range(N)]

    wait_cbc_signal.wait()
    #print("Node %d finishes n-f CBC" % pid)
    #print(is_cbc_delivered)
    #print(cbc_values)

    """
    Run n CBC instance to commit finished CBC IDs
    """

    commit_values = [None] * N

    #assert len(is_cbc_delivered) == N
    #assert sum(is_cbc_delivered) >= N - f
    #assert all(item == 0 or 1 for item in is_cbc_delivered)

    my_commit_input.put_nowait(copy.deepcopy(is_cbc_delivered))  # Deepcopy prevents input changing while executing
    #print("Provide input to commit CBC")

    wait_commit_signal = Event()
    wait_commit_signal.clear()

    def wait_for_commit_to_continue(leader):
        # Receive output from CBC broadcast for commitment
        cl = commit_outputs[leader]()
        if (sum(cl[0]) >= N - f) and all(item == 0 or 1 for item in cl[0]): #
            commit_values[leader] = cl # May block
            is_commit_delivered[leader] = 1
            if sum(is_commit_delivered) >= N - f:
                wait_commit_signal.set()
            #print("Leader %d finishes COMMIT_CBC for node %d" % (leader, pid) )
            #print(is_commit_delivered)

    commit_out_threads = [gevent.spawn(wait_for_commit_to_continue, node) for node in range(N)]

    wait_commit_signal.wait()
    #print("Node %d finishes n-f Commit CBC" % pid)
    #print(is_commit_delivered)
    #print(commit_values)

    """
    Run a Coin instance to permute the nodes' IDs to sequentially elect the leaders
    """
    seed = permutation_coin('permutation')  # Block to get a random seed to permute the list of nodes
    # print(seed)
    np.random.seed(seed)
    pi = np.random.permutation(N)

    # print(pi)

    """
    Repeatedly run biased ABA instances until 1 is output 
    """

    r = 0
    a = None
    votes = defaultdict(set)

    while True:
        #gevent.sleep(0)

        a = pi[r]
        if is_cbc_delivered[a] == 1:
            vote = (a, 1, cbc_values[a])
        else:
            vote = (a, 0, "Bottom")

        for j in range(N):
            send(j, ('VABA_VOTE', r, vote))

        ballot_counter = 0

        while True:
            #gevent.sleep(0)

            sender, msg = vote_recvs[r].get()
            a, ballot_bit, o = msg
            if (pi[r] == a) and (ballot_bit == 0 or ballot_bit == 1):
                if ballot_bit == 1:
                    (m, raw_Sig) = o
                    digestFromLeader = PK1.hash_message(str((sid + 'CBC' + str(a), a, m)))
                    PK1.verify_signature(deserialize1(raw_Sig), digestFromLeader)
                    votes[r].add((sender, msg))
                    ballot_counter += 1
                else:
                    if commit_values[sender] is not None and commit_values[sender][0][a] == 0:
                        votes[r].add((sender, msg))

                        ballot_counter += 1

            if len(votes[r]) >= N - f:
                break

        # print(votes[r])
        aba_r_input = 0
        for vote in votes[r]:
            _, (_, bit, cbc_value) = vote
            if bit == 1:
                aba_r_input = 1
                cbc_values[a] = cbc_value

        def aba_coin_bcast(o):
            """Common coin multicast operation.
            :param o: Value to multicast.
            """
            send(-1, ('VABA_ABA_COIN', r, o))

        coin = shared_coin(sid + 'COIN' + str(r), pid, N, f,
                           PK, SK,
                           aba_coin_bcast, aba_coin_recvs[r].get, logger)

        def make_aba_send(rnd): # this make will automatically deep copy the enclosed send func
            def aba_send(k, o):
                """CBC send operation.
                :param k: Node to send.
                :param o: Value to send.
                """
                # print("node", pid, "is sending", o, "to node", k, "with the leader", j)
                send(k, ('VABA_ABA', rnd, o))
            return aba_send

        # Only leader gets input
        aba = gevent.spawn(baisedbinaryagreement, sid + 'ABA' + str(r), pid, N, f, coin,
                     aba_inputs[r].get, aba_outputs[r].put_nowait,
                     aba_recvs[r].get, make_aba_send(r), logger)
        # aba.get is a blocking function to get aba output
        aba_inputs[r].put_nowait(aba_r_input)
        aba_r = aba_outputs[r].get()
        # print("Round", r, "ABA outputs", aba_r)
        if aba_r == 1:
            break
        r += 1

    assert a is not None
    if logger != None:
        logger.info("VABA %s completes at %s" % (sid, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
    decide(cbc_values[a][0])  # In rare cases, there could return None. We let higher level caller of VABA to deal that
