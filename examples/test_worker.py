######################################################################
#  test_worker.py - An example worker using the Paranoid Pirate
#  pattern in PyProximity.
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

from PyProximity import create_worker
from PyProximity import PP_VALS as PP
from threading import Thread
import zmq
import time

WA = 'BLC07'
WB = 'BLC08'


def run_example():
    worker_a_request = PP.WORKER_REQUEST_URL + '.BLC07'
    worker_a_reply = PP.WORKER_REPLY_URL + '.BLC07'
    worker_b_request = PP.WORKER_REQUEST_URL + '.' + WB
    worker_b_reply = PP.WORKER_REPLY_URL + '.' + WB

    print worker_a_request
    print worker_a_reply
    print worker_b_request
    print worker_b_reply

    try:
        worker_a = create_worker(WA, worker_a_request, worker_a_reply)
        worker_b = create_worker(WB, worker_b_request, worker_b_reply)
        Thread(target=worker_a).start()
        Thread(target=worker_b).start()

        ctx = zmq.Context().instance()
        wa_in_chan = ctx.socket(zmq.PULL)
        wa_out_chan = ctx.socket(zmq.PUSH)
        wb_in_chan = ctx.socket(zmq.PULL)
        wb_out_chan = ctx.socket(zmq.PUSH)
        poller = zmq.Poller()
        poller.register(wa_in_chan, zmq.POLLIN)
        poller.register(wb_in_chan, zmq.POLLIN)
        wa_in_chan.bind(worker_a_request)
        wa_out_chan.connect(worker_a_reply)
        wb_in_chan.bind(worker_b_request)
        wb_out_chan.connect(worker_b_reply)

        while True:
            socks = dict(poller.poll())

            # A work request for worker A
            if socks.get(wa_in_chan) == zmq.POLLIN:
                frames = wa_in_chan.recv_multipart()
                request = frames.pop()
                frames.append('I got a request: \'%s\'' % request)
                wa_out_chan.send_multipart(frames)

            # A work request for worker B
            if socks.get(wb_in_chan) == zmq.POLLIN:
                frames = wb_in_chan.recv_multipart()
                request = frames.pop()
                frames.append(
                    'Hi, I\'m another worker, and I too got a request: \'%s\''
                    % request)
                # sleep for some time. Heartbeating will continue,
                # this will not block the worker thread.
                time.sleep(10)
                wb_out_chan.send_multipart(frames)
    except KeyboardInterrupt:
        print("W: KeyboardInterrupt received, stopping...")
    except:
        print("E: got some other interrupt, stopping...")
    finally:
        wa_in_chan.close()
        wa_out_chan.close()
        wb_in_chan.close()
        wb_out_chan.close()
        return True
