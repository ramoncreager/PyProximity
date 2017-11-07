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
from PyProximity import JSONEncoder
from PyProximity import PP_VALS as PPP


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


def create_publisher(router_backside_url, pub_type,
                     worker_id, encoder=JSONEncoder):
    """Creates a publisher of a given publication type.  Publication types
    make sense to the publisher and the subscribers.

    *router_backside_url* This URL is to the router's backside. It
     will receive the data and publish it over a well known zmq.PUB
     socket.

    *pub_type*: The publication type. This may be used by the
     publisher and the client to categorize the the publication.

    *worker_id*: The name of the worker: 'PLAYER01', 'BLC13', etc.

    *encoder*: The encoding/decoding class. Defaults to JSONEncoder,
     may also be MsgPackEncoder.

    """
    ctx = zmq.Context().instance()
    pipe = ctx.socket(zmq.DEALER)
    pipe.connect(router_backside_url)
    e = encoder()
    mtypes = {PPP.LOG: 'LOG', PPP.ALERT: 'ALERT', PPP.SAMPLE: 'SAMPLE'}

    def pub(key, msg):
        """Publishes the data 'msg' using the key 'key'. 'key' is in the
        format 'type:name' and will be converted by this function into
        'type:worker:name'

        """
        packed = e.encode(msg)
        frames = ['%s:%s:%s' % (mtypes[pub_type], worker_id, key),
                  pub_type, packed]
        pipe.send_multipart(frames)

    return pub


def subscribe(sub_url, key, encoder=JSONEncoder):
    ctx = zmq.Context().instance()
    sub = ctx.socket(zmq.SUB)
    poller = zmq.Poller()
    enc = encoder()
    poller.register(sub, zmq.POLLIN)
    sub.connect(sub_url)
    sub.setsockopt(zmq.SUBSCRIBE, key)

    while True:
        try:
            socks = dict(poller.poll())
            if sub in socks:
                msg = sub.recv_multipart()
                unpacked = enc.decode(msg[-1])
                msg.pop()
                msg.append(unpacked)
                print msg
        except KeyboardInterrupt:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            break
