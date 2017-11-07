######################################################################
#  pp_logging_handler.py - A logger.Handler class that sends a message
#  using the 'log' functionality of the PP proxy server.
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


import logging
from pprint import pprint


class FakeProxy:
    '''Allows for easy testing of PPHandler'''

    def __init__(self, id):
        self.id = id

    def log(self, key, val):
        pprint(key)
        pprint(val)


class PPHandler(logging.Handler):
    '''A logging module handler that works with the Paranoid Pirate
       protocol.

    '''

    def __init__(self, proxy, level=logging.NOTSET):
        self.proxy = proxy
        super(PPHandler, self).__init__(level)

    def __del__(self):
        self.close()

    def createLock(self):
        pass

    def acquire(self):
        pass

    def release(self):
        pass

    def flush(self):
        pass

    def close(self):
        pass

    def emit(self, record):
        if record:
            message = record.__dict__
            message['player'] = self.proxy.id
            message['message'] = message['msg'] % message['args']
            key = 'LOG:%s' % self.proxy.id
            self.proxy.log(key, message)
