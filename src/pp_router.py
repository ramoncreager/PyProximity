

# encoding: utf-8
#
#   Custom routing Router to Dealer
#
#   This is based on the original rtdealer.py example in The Guide
#   (http://zguide.zeromq.org/py:rtdealer) by Jeremy Avnet (brainsik)
#   <spork(dash)zmq(at)theory(dot)org>
#
#   I have tried to make the workers more general, so that you can
#   spawn as many as you'd like. Just modify the 'workers' list in
#   'run_tasks(). Also, to demonstrate the 2-way asynchronous
#   communication channel that this approach provides over REQ/REP,
#   the Broker does the bookkeeping of the worker activity.
#
#   R. Creager <rcreager(at)nrao(dot)edu>
#

import time
import zmq
from pp_config import PP_VALS as PPP
from collections import OrderedDict


class Worker(object):

    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + \
            PPP.HEARTBEAT_INTERVAL * \
            PPP.HEARTBEAT_LIVENESS


class WorkerQueue(object):

    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address, worker in self.queue.iteritems():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print "W: Idle worker expired: %s" % address
            self.queue.pop(address, None)

        return expired

    def next(self):
        address, worker = self.queue.popitem(False)
        return address


def broker():
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    pipe = context.socket(zmq.REP)

    frontend.bind(PPP.FRONTEND_SERVER_URL)
    backend.bind(PPP.BACKEND_SERVER_URL)
    pipe.bind(PPP.ROUTER_CONTROL_URL)
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    poller.register(pipe, zmq.POLLIN)
    workers = WorkerQueue()
    heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
    pending_requests = dict()

    # these help in disconnecting clients of workers who went away
    # prior to being hearbeated out.
    def pending_push(worker, client):
        """called by the frontend if it thinks worker exists"""
        if worker not in pending_requests:
            pending_requests[worker] = set()

        pending_requests[worker].add(client)

    def pending_pop(workers, msg=True):
        """called by the backend if it is returning a frontend call, or if it
           is purging the workers

        """
        for w in workers:
            if w in pending_requests:
                if msg:
                    for c in pending_requests[w]:
                        frontend.send_multipart([c, PPP.NO_SUCH_WORKER])

                pending_requests.pop(w)

    while True:
        socks = dict(poller.poll(PPP.HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on BACKEND
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            print "Backend - worker: %s" % str(frames)

            if not frames:
                continue

            address = frames[0]
            workers.ready(Worker(address))

            # Validate control message, or return reply to client
            msg = frames[1:]

            if len(msg) == 1:
                if msg[0] not in (PPP.READY, PPP.HEARTBEAT):
                    print "Backend - E: Invalid message from worker: %s" \
                        % str(frames)
                else:
                    print "Backend - I: Got message %s from worker" % str(msg)
                    backend.send_multipart([address, PPP.HEARTBEAT])
            else:
                frontend.send_multipart(msg)
                pending_pop([address], False)

            # Send heartbeats to idle workers if it's time
            if time.time() >= heartbeat_at:
                for worker in workers.queue:
                    print "Backend - sending HB to worker %s" % str(worker)
                    msg = [worker, PPP.HEARTBEAT]
                    backend.send_multipart(msg)
                heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL

        # Handle activity from FRONTEND
        if socks.get(frontend) == zmq.POLLIN:
            frames = frontend.recv_multipart()
            print "Frontend - request: %s" % str(frames)
            if not frames:
                continue

            flen = len(frames)

            if flen == 3:
                id = frames[1]

                if id in workers.queue:
                    pending_push(id, frames[0])
                    frontend.send_multipart([frames[0], PPP.REQ_ACK])
                    frames.insert(0, id)  # I want specified worker
                    backend.send_multipart(frames)
                else:
                    frontend.send_multipart(
                        [frames[0], PPP.NO_SUCH_WORKER])

        if socks.get(pipe) == zmq.POLLIN:
            msg = pipe.recv_multipart()
            print "PIPE - got %s" % str(msg)

            if msg[0] == PPP.QUIT:
                reply = b"Router is quitting."
                pipe.send(reply)
                print "Router is quitting."
                return
            elif msg[0] == PPP.KILL_WORKERS:
                for w in workers.queue:
                    backend.send_multipart([w, PPP.QUIT])

                reply = b"All workers sent the QUIT message"
                pipe.send(reply)
            else:
                reply = b"%s: did not understand request." % msg
                pipe.send(reply)

        purged = set(workers.purge())
        pending_pop(purged)


def router_ctl(cmd):
    ctx = zmq.Context().instance()
    pipe = ctx.socket(zmq.REQ)
    pipe.connect("tcp://localhost:5557")
    pipe.send(cmd)
    msg = pipe.recv()
    pipe.setsockopt(zmq.LINGER, 0)
    pipe.close()
    return msg


def client_msg(addr, msg):
    ctx = zmq.Context().instance()
    pipe = ctx.socket(zmq.DEALER)
    poller = zmq.Poller()
    poller.register(pipe, zmq.POLLIN)
    pipe.connect("tcp://localhost:5555")
    pipe.send_multipart([addr, msg])
    socks = dict(poller.poll(3 * 1000))
    reply = []

    if pipe in socks:
        msg = pipe.recv_multipart()

        if msg[-1] == PPP.NO_SUCH_WORKER:
            print "Router reports no such worker"
            # and that's the end of that.
        elif msg[-1] == PPP.REQ_ACK:
            reply = pipe.recv_multipart()

            if len(reply) == 1 and reply[0] == PPP.NO_SUCH_WORKER:
                print "Router just removed the worker."
    else:
        print "Router is not answering."

    pipe.setsockopt(zmq.LINGER, 0)
    pipe.close()
    return reply
