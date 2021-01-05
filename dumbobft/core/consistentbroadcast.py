import time
from datetime import datetime

import gevent
from collections import defaultdict

from gevent import monkey

from crypto.threshsig.boldyreva import serialize, deserialize1

def consistentbroadcast(sid, pid, N, f, PK1, SK1, leader, input, receive, send, logger=None):
    """Consistent broadcast
    :param str sid: session identifier
    :param int pid: ``0 <= pid < N``
    :param int N:  at least 3
    :param int f: fault tolerance, ``N >= 3f + 1``
    :param PK1: ``boldyreva.TBLSPublicKey`` with threshold n-f
    :param SK1: ``boldyreva.TBLSPrivateKey`` with threshold n-f
    :param int leader: ``0 <= leader < N``
    :param input: if ``pid == leader``, then :func:`input()` is called
        to wait for the input value
    :param receive: :func:`receive()` blocks until a message is
        received; message is of the form::

            (i, (tag, ...)) = receive()

        where ``tag`` is one of ``{"VAL", "ECHO", "READY"}``
    :param send: sends (without blocking) a message to a designed
        recipient ``send(i, (tag, ...))``

    :return str: ``m`` after receiving ``CBC-FINAL`` message
        from the leader

        .. important:: **Messages**

            ``CBC_VAL( m )``
                sent from ``leader`` to each other party
            ``CBC_ECHO( m, sigma )``
                sent to leader after receiving ``CBC-VAL`` message
            ``CBC_FINAL( m, Sigma )``
                sent from ``leader`` after receiving :math:``N-f`` ``CBC_ECHO`` messages
                where Sigma is computed over {sigma_i} in these ``CBC_ECHO`` messages
    """

    assert N >= 3*f + 1
    assert f >= 0
    assert 0 <= leader < N
    assert 0 <= pid < N
    assert PK1.k == N - f
    assert PK1.l == N

    EchoThreshold = N - f      # Wait for this many CBC_ECHO to send CBC_FINAL
    digestFromLeader = None
    finalSent = False
    cbc_echo_sshares = defaultdict(lambda: None)

    #print("CBC starts...")

    if pid == leader:
        # The leader sends the input to each participant
        #print("block to wait for CBC input")
        def wait_for_input():
            m = input() # block until an input is received
            if logger != None:
                logger.info("CBC %s get input at %s" % (sid, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
            #print("CBC input received: ", m)
            #assert isinstance(m, (str, bytes, list, tuple))
            digestFromLeader = PK1.hash_message(str((sid, leader, m)))
            # print("leader", pid, "has digest:", digestFromLeader)
            cbc_echo_sshares[pid] = SK1.sign(digestFromLeader)
            send(-1, ('CBC_SEND', m))
            #print("Leader %d broadcasts CBC SEND messages" % leader)

        gevent.spawn(wait_for_input)

    # Handle all consensus messages
    while True:
        #gevent.sleep(0.0001)

        (j, msg) = receive()
        #print("recv3", (j, msg))

        if msg[0] == 'CBC_SEND' and digestFromLeader is None:
            # CBC_SEND message
            (_, m) = msg
            if j != leader:
                print("Node %d receives a CBC_SEND message from node %d other than leader %d" % (pid, j, leader), msg)
                continue
            digestFromLeader = PK1.hash_message(str((sid, leader, m)))
            #print("Node", pid, "has digest:", digestFromLeader, "for leader", leader, "session id", sid, "message", m)
            send(leader, ('CBC_ECHO', m, serialize(SK1.sign(digestFromLeader))))

        elif msg[0] == 'CBC_ECHO':
            # CBC_READY message
            #print("I receive CBC_ECHO from node %d" % j)
            if pid != leader:
                print("I reject CBC_ECHO from %d as I am not CBC leader:", j)
                continue
            (_, m, raw_sigma) = msg
            sigma = deserialize1(raw_sigma)
            try:
                digest = PK1.hash_message(str((sid, leader, m)))
                assert PK1.verify_share(sigma, j, digest)
            except AssertionError:
                print("Signature share failed in CBC!", (sid, pid, j, msg))
                continue
            #print("I accept CBC_ECHO from node %d" % j)
            cbc_echo_sshares[j] = sigma
            if len(cbc_echo_sshares) >= EchoThreshold and not finalSent:
                sigmas = dict(list(cbc_echo_sshares.items())[:N - f])
                Sigma = PK1.combine_shares(sigmas)
                # assert PK.verify_signature(Sigma, digestFromLeader)
                finalSent = True
                send(-1, ('CBC_FINAL', m, serialize(Sigma)))
                #print("Leader %d broadcasts CBC FINAL messages" % leader)

        elif msg[0] == 'CBC_FINAL':
            # CBC_FINAL message
            #print("I receive CBC_FINAL from node %d" % j)
            if j != leader:
                print("Node %d receives a CBC_FINAL message from node %d other than leader %d" % (pid, j, leader), msg)
                continue
            (_, m, raw_Sigma) = msg
            Sigma = deserialize1(raw_Sigma)
            try:
                digest = PK1.hash_message(str((sid, leader, m)))
                assert PK1.verify_signature(Sigma, digest)
                #print("Valid CBC Final received...")
            except AssertionError:
                print("Signature failed!", (sid, pid, j, msg))
                continue
            #print("CBC finished for leader", leader)
            if logger != None:
                logger.info("CBC %s completes at %s" % (sid, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))

            return m, raw_Sigma
