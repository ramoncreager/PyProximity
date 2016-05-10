######################################################################
#  pp_router.py - PyProximity router. Receives requests from a client,
#  passes them along to the requested worker, or returns an error if
#  the requested worker is not running.
#
#  This is based on the original rtdealer.py example in The Guide
#  (http://zguide.zeromq.org/py:rtdealer) by Jeremy Avnet (brainsik)
#  <spork(dash)zmq(at)theory(dot)org>, with changes needed to make it
#  work with named workers and to report worker outages. The original
#  was more of a load-balancing approach, where any available worker
#  would do. This code assumes wokers that have a specific identity
#  because they are not interchangeable (for example, tied to
#  hardware).
#
#  Copyright (C) 2016 Associated Universities, Inc. Washington DC, USA.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
#  Correspondence concerning GBT software should be addressed as follows:
#  GBT Operations
#  National Radio Astronomy Observatory
#  P. O. Box 2
#  Green Bank, WV 24944-0002 USA
#
######################################################################


import time
import zmq
import logging as log

from PyProximity import PP_VALS as PPP
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
            log.warning("Idle worker expired: %s", address)
            self.queue.pop(address, None)

        return expired

    def next(self):
        address, worker = self.queue.popitem(False)
        return address


def broker(front_url=None, back_url=None, ctrl_url=None):
    """Runs the broker at the specified URLs. The broker is the central
    connection point to the system. All of its interfaces are ZMQ
    server interfaces.

    front_url: The front-facing interface's URL. Clients to the system
    will connect to this. This interface is a ZMQ ROUTER

    back_url: The back-facing interface's URL. Workers will connect to
    this. This interface is a ZMQ ROUTER.

    ctrl_url: This is a control interface, can tell the router to do
    things such as kill workers, kill itself. This is a ZMQ REP interface.

    """
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    pipe = context.socket(zmq.REP)

    if not front_url:
        front_url = PPP.FRONTEND_SERVER_URL

    if not back_url:
        back_url = PPP.BACKEND_SERVER_URL

    if not ctrl_url:
        ctrl_url = PPP.ROUTER_CONTROL_URL

    frontend.bind(front_url)
    backend.bind(back_url)
    pipe.bind(ctrl_url)
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
            log.info("Backend: %s", frames)

            if not frames:
                continue

            address = frames[0]
            workers.ready(Worker(address))

            # Validate control message, or return reply to client
            msg = frames[1:]

            if len(msg) == 1:
                if msg[0] not in (PPP.READY, PPP.HEARTBEAT):
                    log.error("Backend: Invalid message: '%s' from worker %s",
                              address, str(frames))
                else:
                    log.debug("Backend: Got message %s from worker %s",
                              msg, address)
                    backend.send_multipart([address, PPP.HEARTBEAT])
            else:
                log.debug("Frontend: Sending to client", msg)
                frontend.send_multipart(msg)
                pending_pop([address], False)

            # Send heartbeats to idle workers if it's time
            if time.time() >= heartbeat_at:
                for worker in workers.queue:
                    log.debug("Backend - sending HB to worker %s", worker)
                    msg = [worker, PPP.HEARTBEAT]
                    backend.send_multipart(msg)
                heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL

        # Handle activity from FRONTEND
        if socks.get(frontend) == zmq.POLLIN:
            frames = frontend.recv_multipart()
            log.info("Frontend: %s", frames)
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
                    log.warning('%s: NO_SUCH_WORKER', id)

        if socks.get(pipe) == zmq.POLLIN:
            msg = pipe.recv_multipart()
            log.debug("PIPE - got %s", msg)

            if msg[0] == PPP.QUIT:
                reply = b"Router is quitting."
                pipe.send(reply)
                log.info("Router is quitting.")
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
