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
from socket import gethostname
import logging
import sys
import traceback
import inspect
from PyProximity import PP_VALS as PPP
from PyProximity import PyProximityException

log = logging.getLogger('pp_worker')


def formatExceptionInfo(maxTBlevel=5):
    """
    Obtains information from the last exception thrown and extracts
    the exception name, data and traceback, returning them in a tuple
    (string, string, [string, string, ...]).  The traceback is a list
    which will be 'maxTBlevel' deep.
    """
    cla, exc, trbk = sys.exc_info()
    excName = cla.__name__
    excArgs = exc.__str__()
    excTb = traceback.format_tb(trbk, maxTBlevel)
    return (excName, excArgs, excTb)


def printException(formattedException):
    """
    Takes the tuple provided by 'formatExceptionInfo' and prints it
    out exactly as an uncaught exception would be in an interactive
    python shell.
    """
    print "Traceback (most recent call last):"

    for i in formattedException[2]:
        print i,

    print "%s: %s" % (formattedException[0], formattedException[1])


def create_worker(identity, broker_url,
                  req_url, rep_url, ctrl_url, context=None):
    """Creates a worker, closing over 'identity', 'req_chan', 'req_chan',
       'context'.

    identity: The name of the worker. This should be unique within the
    system, to allow the client to specify this worker.

    req_url: If provided, requests from the client are placed in this queue.

    rep_url: If provided, results from the request are placed in this queue

    context: If provided, this context will be used to create and
    operate ZMQ objects; otherwise the global instance will be used.

    """

    hostname = gethostname()

    def worker_socket(id, context, poller):
        if not id:
            raise PyProximityException("id passed to worker is invalid: '%s'"
                                       % (id))

        worker = context.socket(zmq.DEALER)
        worker.setsockopt(zmq.IDENTITY, id)
        poller.register(worker, zmq.POLLIN)
        worker.connect(broker_url)
        worker.send_multipart([PPP.READY, hostname])
        return worker

    def worker():
        try:
            id = identity
            ctx = context or zmq.Context.instance()
            work_request = ctx.socket(zmq.PUSH)
            work_result = ctx.socket(zmq.PULL)
            worker_ctl = ctx.socket(zmq.REP)
            poller = zmq.Poller()
            liveness = PPP.HEARTBEAT_INTERVAL
            interval = PPP.INTERVAL_INIT
            router_clnt = worker_socket(id, ctx, poller)
            heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
            poller.register(work_result, zmq.POLLIN)
            poller.register(worker_ctl, zmq.POLLIN)
            worker_ctl.bind(ctrl_url)
            work_request.connect(req_url)
            work_result.bind(rep_url)

            while True:
                socks = dict(poller.poll(PPP.HEARTBEAT_INTERVAL * 1000))

                #############################################
                # A request from the broker:
                #############################################
                if socks.get(router_clnt) == zmq.POLLIN:
                    #  Get message
                    #  - 3-part envelope + content -> request
                    #  - 1-part HEARTBEAT -> heartbeat
                    frames = router_clnt.recv_multipart()
                    log.debug("%s - YYYYAAAAAYYYYY!!!! got frame: %s",
                              id, frames)

                    if not frames:
                        break  # Interrupted

                    flen = len(frames)

                    if flen == 1:
                        if frames[0] == PPP.HEARTBEAT:
                            log.debug("%s: Queue heartbeat", id)
                            liveness = PPP.HEARTBEAT_LIVENESS
                        elif frames[0] == PPP.QUIT:
                            log.info("%s: Queue terminating, request from Broker",
                                     id)
                            work_request.send(PPP.QUIT)
                            break
                        else:
                            reply = b"%s: Did not understand '%s'" % (
                                id, frames)
                            router_clnt.send_multipart([reply])
                    else:
                        log.debug("%s: Normal reply", id)
                        log.debug("%s - %s", id, frames)

                        # channel to code that actually does work
                        log.debug("%s: Posting request", id)
                        work_request.send_multipart(frames)
                        liveness = PPP.HEARTBEAT_LIVENESS

                    interval = PPP.INTERVAL_INIT

                #############################################
                # A result from the work code (pp_proxy.py):
                #############################################
                elif socks.get(work_result) == zmq.POLLIN:
                    frames = work_result.recv_multipart()
                    log.debug("WORKER - I: Received result! %s", frames)
                    router_clnt.send_multipart(frames)

                #############################################
                # A request from the work code (pp_proxy.py):
                #############################################
                elif socks.get(worker_ctl) == zmq.POLLIN:
                    msg = worker_ctl.recv()

                    if msg == PPP.QUIT:
                        log.debug("%s - I: Queue terminating, "
                                  "request from code"
                                  % id)
                        worker_ctl.send(PPP.QUIT)
                        break

                #############################################
                # timeout
                #############################################
                else:
                    liveness -= 1
                    if liveness == 0:
                        log.debug(
                            "%s - W: Heartbeat failure, can't reach queue"
                            % id)
                        log.debug("%s - W: Reconnecting in %0.2fs..." %
                                  (id, interval))
                        time.sleep(interval)

                        if interval < PPP.INTERVAL_MAX:
                            interval *= 2

                        poller.unregister(router_clnt)
                        router_clnt.setsockopt(zmq.LINGER, 0)
                        router_clnt.close()
                        router_clnt = worker_socket(id, ctx, poller)
                        liveness = PPP.HEARTBEAT_LIVENESS

                if time.time() > heartbeat_at:
                    heartbeat_at = time.time() + PPP.HEARTBEAT_INTERVAL
                    log.debug("%s@%s - I: Worker heartbeat" % (id, hostname))
                    router_clnt.send_multipart([PPP.HEARTBEAT, hostname])
        except:
            log.fatal("Something very bad happened.")
            printException(formatExceptionInfo())
            sys.exit()
        finally:
            worker_ctl.setsockopt(zmq.LINGER, 0)
            worker_ctl.close()
            work_request.setsockopt(zmq.LINGER, 0)
            work_request.close()
            work_result.setsockopt(zmq.LINGER, 0)
            work_result.close()
            router_clnt.setsockopt(zmq.LINGER, 0)
            router_clnt.close()

    return worker
