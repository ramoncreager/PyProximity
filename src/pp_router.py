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
import json
import logging

from PyProximity import PP_VALS as PPP
from collections import OrderedDict

log = logging.getLogger('pp_router')


class Worker(object):

    def __init__(self, address, host):
        self.address = address
        self.expiry = time.time() + \
            PPP.HEARTBEAT_INTERVAL * \
            PPP.HEARTBEAT_LIVENESS
        self.hostname = host


class WorkerQueue(object):

    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker, pub_sock):
        '''Refresh the active worker queue'''
        # If the worker does not exists, attempting to pop it off the
        # queue will return None. And if it does not exists, publish
        # that we are adding a new worker.
        if not self.queue.pop(worker.address, None):
            log.info("New worker %s added", worker.address)
            worker_info = [worker.address, worker.hostname]
            payload = json.dumps(worker_info)
            msg = ['Broker:NewWorker', payload]
            pub_sock.send_multipart(msg)

        self.queue[worker.address] = worker

    def purge(self, pub_sock):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address, worker in self.queue.iteritems():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
                # Publish the fact that a worker went away.
                payload = json.dumps(address)
                msg = ['Broker:ExpiredWorker', payload]
                pub_sock.send_multipart(msg)

        for address in expired:
            log.warning("Idle worker expired: %s", address)
            self.queue.pop(address, None)

        return expired

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

    def get_host(self, address):
        if address in self.queue:
            return self.queue[address].hostname
        return 'unknown'

    def get_hosts(self):
        result = list()

        for w in self.queue:
            result.append([w, self.queue[w].hostname])

        return result


def broker(front_url=None, back_url=None, ctrl_url=None, pub_url=None):
    """Runs the broker at the specified URLs. The broker is the central
    connection point to the system. All of its interfaces are ZMQ
    server interfaces.

    front_url: The front-facing interface's URL. Clients to the system
    will connect to this. This interface is a ZMQ ROUTER

    back_url: The back-facing interface's URL. Workers will connect to
    this. This interface is a ZMQ ROUTER.

    ctrl_url: This is a control interface, can tell the router to do
    things such as kill workers, kill itself. This is a ZMQ REP interface.

    pub_url: This is a zmq.PUB interface. Samplers, Alerts and Logs
    are published via this interface.

    """
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    pipe = context.socket(zmq.REP)
    pub = context.socket(zmq.PUB)

    if not front_url:
        front_url = PPP.FRONTEND_SERVER_URL

    if not back_url:
        back_url = PPP.BACKEND_SERVER_URL

    if not ctrl_url:
        ctrl_url = PPP.ROUTER_CONTROL_URL

    if not pub_url:
        pub_url = PPP.ROUTER_PUB_URL

    frontend.bind(front_url)
    backend.bind(back_url)
    pipe.bind(ctrl_url)
    pub.bind(pub_url)
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    poller.register(pipe, zmq.POLLIN)
    workers = WorkerQueue()
    heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
    pending_requests = dict()
    broker_done = False

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

    internal_protocols = (PPP.READY, PPP.HEARTBEAT, PPP.GET_HOSTS,
                          PPP.ALERT, PPP.SAMPLE, PPP.LOG)

    while not broker_done:
        try:
            socks = dict(poller.poll(PPP.HEARTBEAT_INTERVAL * 1000))

            # Handle worker activity on BACKEND
            if socks.get(backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                frames = backend.recv_multipart()
                log.debug("Backend - frames: %s", frames)

                if not frames:
                    continue

                address = frames[0]
                # Validate control message, or return reply to client
                msg = frames[1:]

                try:
                    if len(msg) <= 3:
                        if msg[0] not in internal_protocols:
                            log.error(
                                "Backend: Invalid message: '%s' "
                                "from worker %s",
                                address, str(frames))
                        elif msg[0] in (PPP.READY, PPP.HEARTBEAT):
                            log.debug("Backend: Got message %s from worker %s",
                                      msg, address)
                            backend.send_multipart([address, PPP.HEARTBEAT])
                            workers.ready(Worker(address, msg[1]), pub)
                        elif msg[0] == PPP.GET_HOSTS:
                            hosts = json.dumps(workers.get_hosts())
                            backend.send_multipart(
                                [address, PPP.GET_HOSTS, hosts])
                        elif msg[0] == PPP.ALERT \
                                or msg[0] == PPP.SAMPLE \
                                or msg[0] == PPP.LOG:
                            pub.send_multipart(msg[1:])
                    else:
                        msg_type = msg[-2]
                        log.debug("Message from Worker. Type: %s, %s",
                                  msg_type, msg)

                        if msg_type == PPP.RPC:
                            log.debug("Frontend: Sending to client %s", msg)
                            frontend.send_multipart(msg)
                            pending_pop([address], False)

                            # rpc calls don't send the hostname, only
                            # heartbeat does. So obtain previously cached
                            # hostname.
                            hostname = workers.get_host(address)
                            workers.ready(Worker(address, hostname), pub)
                        else:
                            log.warning(
                                "Backend: Unknown message type %s", msg)
                except IndexError as e:
                    # This will happen if there are version mismatches
                    # between worker and router which result in having the
                    # right protocol, but the wrong number of frames.
                    log.exception("Protocol error: %s", e)

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
                log.debug("Frontend: %s", frames)
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
                            [frames[0], id, PPP.NO_SUCH_WORKER])
                        log.warning('%s: NO_SUCH_WORKER', id)
                elif flen == 2:
                    if frames[1] == PPP.GET_HOSTS:
                        frames.append(json.dumps(workers.get_hosts()))
                        frontend.send_multipart(frames)

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

            purged = set(workers.purge(pub))
            pending_pop(purged)
        except KeyboardInterrupt:
            print
            print "Broker terminating."
            broker_done = True
