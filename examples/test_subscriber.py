######################################################################
#  test_subscriber.py - Test program to subscribe from a PyProximity
#  Broker.
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
import argparse
from PyProximity import JSONEncoder, MsgPackEncoder


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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Runs a PyProximity sample subscriber.')
    parser.add_argument('-u', '--url', default='',
                        help='URL to subscribe from',
                        type=str)
    parser.add_argument('-k', '--key', default='',
                        help='The key to subscribe to.',
                        type=str)
    parser.add_argument('-m', '--MsgPack', default=False,
                        help='If true use MsgPack; if not defaults to JSON',
                        action='store_true')
    args = parser.parse_args()

    url = args.url
    key = args.key

    if args.MsgPack:
        Encoder = MsgPackEncoder
    else:
        Encoder = JSONEncoder

    subscribe(url, key, Encoder)
