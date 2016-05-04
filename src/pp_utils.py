######################################################################
#  pp_utils.py - Utilities for the PyProximity Paranoid Pirate pattern.
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

import zmq
from PyProximity import PP_VALS as PPP


class PyProximityException(Exception):

    def __init__(self, message):
        Exception.__init__(self, message)


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
