######################################################################
#  pp_worker.py - implements the worker portion of the Paranoid Pirate
#  Protocol for PyProximity
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
from ppp_config import PP_VALS as PPP


def create_worker(identity, req_chan, rep_chan, context=None):
    """Creates a worker, closing over 'identity', 'req_chan', 'req_chan',
       'context'.

    identity: The name of the worker. This should be unique within the
    system, to allow the client to specify this worker.

    req_chan: If provided, requests from the client are placed in this queue.

    rep_chan: If provided, results from the request are placed in this queue

    context: If provided, this context will be used to create and
    operate ZMQ objects; otherwise the global instance will be used.

    """

    def worker_socket(id, context, poller):
        worker = context.socket(zmq.DEALER)
        worker.setsockopt(zmq.IDENTITY, id)
        poller.register(worker, zmq.POLLIN)
        worker.connect("tcp://localhost:5556")
        worker.send(PPP.READY)
        return worker

    def worker():
        id = identity
        ctx = context or zmq.Context.instance()
        to_worker = ctx.socket(zmq.PUSH)
        work_request = ctx.socket(zmq.PUSH)
        work_result = ctx.socket(zmq.PULL)
        poller = zmq.Poller()
        liveness = PPP.HEARTBEAT_INTERVAL
        interval = PPP.INTERVAL_INIT
        worker = worker_socket(id, ctx, poller)
        heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
        # work_request.connect(WORKER_REPLY_URL)
        # work_result.bind(WORKER_REPLY_URL)
        poller.register(work_result, zmq.POLLIN)

        while True:
            socks = dict(poller.poll(PPP.HEARTBEAT_INTERVAL * 1000))

            if socks.get(worker) == zmq.POLLIN:
                #  Get message
                #  - 3-part envelope + content -> request
                #  - 1-part HEARTBEAT -> heartbeat
                frames = worker.recv_multipart()
                print "%s - YYYYAAAAAYYYYY!!!! got frame: %s" % \
                    (id, str(frames))

                if not frames:
                    break  # Interrupted

                flen = len(frames)

                if flen == 3:
                    print "%s - I: Normal reply" % id
                    print "%s - %s" % (id, str(frames))

                    # channel to code that actually does work
                    if req_chan:
                        req_chan.put(frames)
                    # reply from code that actually does work
                    if rep_chan:
                        frames = rep_chan.get()

                    worker.send_multipart(frames)
                    liveness = PPP.HEARTBEAT_LIVENESS
                elif flen == 1:
                    if frames[0] == PPP.HEARTBEAT:
                        print "%s - I: Queue heartbeat" % id
                        liveness = PPP.HEARTBEAT_LIVENESS
                    elif frames[0] == PPP.QUIT:
                        print "%s - I: Queue terminating" % id
                        break
                    else:
                        reply = b"%s: Did not understand '%s'" % (id, frames)
                        worker.send_multipart([reply])
                else:
                    print "%s - E: Invalid message: %s" % (id, str(frames))
                interval = PPP.INTERVAL_INIT
            else:
                liveness -= 1
                if liveness == 0:
                    print "%s - W: Heartbeat failure, can't reach queue" % id
                    print "%s - W: Reconnecting in %0.2fs..." % (id, interval)
                    time.sleep(interval)

                    if interval < PPP.INTERVAL_MAX:
                        interval *= 2

                    poller.unregister(worker)
                    worker.setsockopt(zmq.LINGER, 0)
                    worker.close()
                    worker = worker_socket(id, ctx, poller)
                    liveness = PPP.HEARTBEAT_LIVENESS

            if time.time() > heartbeat_at:
                heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
                print "%s - I: Worker heartbeat" % id
                worker.send(PPP.HEARTBEAT)

    return worker
