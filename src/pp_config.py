######################################################################
#  pp_config.py - Configuration constants and file reader for PyProximity.
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


class PP_VALS:
    HEARTBEAT_INTERVAL = 5.0     # Heartbeat interval, in seconds
    HEARTBEAT_LIVENESS = 3       # 3 heartbeats missed to purge worker.
    INTERVAL_INIT = 1
    INTERVAL_MAX = 32

    #  Paranoid Pirate Protocol constants
    READY = "\x01"           # Signals worker is ready
    HEARTBEAT = "\x02"       # Signals worker heartbeat
    REQ_ACK = "\x03"         # ACK to client that message handled
    NO_SUCH_WORKER = "\x04"  # Message to client that no such worker exists
    RPC = '\x05'             # The message is an RPC response
    ALERT = '\0x06'          # The message is an alert (an M&C Message equiv.)
    SAMPLE = '\0x07'         # The message is a data sample publication
    LOG = '\0x08'            # The message is a log message
    QUIT = "\x10"            # Kill the worker.
    KILL_WORKERS = "\x11"    # tells router to kill all connected workers.

    # endpoints
    FRONTEND_SERVER_URL = "tcp://*:5555"
    BACKEND_SERVER_URL = "tcp://*:5556"
    ROUTER_CONTROL_URL = "tcp://*:5557"
    ROUTER_PUB_URL = 'tcp://*:5558'

    # worker inproc channels. These are a pair of PUSH/PULL sockets,
    # and a control channel. They are the channels to/from the actual
    # code that does the requested work. The worker will post the
    # request using the WORKER_REQUEST_URL and continue on. The worker
    # will asynchronously perform the work, and post the reply to the
    # WORKER_REPLY_URL chanel, to be returned to the client. When the
    # user is done with the worker it can kill it via the
    # WORKER_CONTROL_URL.
    WORKER_REQUEST_URL = "ipc://worker_request"
    WORKER_REPLY_URL = "ipc://worker_reply"
    WORKER_CONTROL_URL = "ipc://worker_control"
