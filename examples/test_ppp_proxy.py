######################################################################
#  test_simple_proxy.py - An example simple proxy.
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
import time
import signal
import logging as log

from PyProximity import PPPProxyServer
from zmq.error import ZMQError


class Animals:

    def __init__(self):
        self.angry = self.Angry()

    class Angry:

        def cat(self):
            """
            Angry Cat.
            """
            print "hiss"
            return "hiss"

        def dog(self):
            """
            Angry Dog.
            """
            print "growl"
            return "growl"

        # def frog(self):
        #     """
        #     Angry Frog
        #     """
        #     print "piss"
        #     return "piss"

    def cat(self):
        """
        Cat process.
        """
        print "meow"
        return "meow"

    def dog(self):
        """
        Dog process.
        """
        print "woof"
        return "woof"

    def frog(self):
        """
        Frog process.
        """
        print "rivet!"
        return "rivet!"


class MoreInteresting:

    def add_two(self, x, y):
        """
        Adds two values together.

        x: first value
        y: second value

        returns: x + y
        """
        print "returning %d + %d = %d" % (x, y, x + y)
        return x + y

    def div_two(self, x, y):
        print "returning %d / %d = %d" % (x, y, x / y)
        return x / y

    def sub_two(self, x, y):
        print "returning %d - %d = %d" % (x, y, x - y)
        return x - y

    def long_delay(self, delay):
        """
        waits 'delay' seconds before returning.

        delay: the delay in seconds
        """
        print "sleeping for", delay, "seconds"
        time.sleep(delay)
        return delay

    def bad_delay(self, delay):
        """This is like 'long_delay()', except that it expects a
        datetime.timedelta(). This demonstrates the limitations of using
        the built-in JSON encoding in PyZMQ. Calling this function with
        a delay as a timedelta will generate a TypeError exception
        complaining that timedelta is not JSON serializable.

        """
        print "sleeping for", delay.seconds, "seconds"
        time.sleep(delay.seconds)
        return delay

    def complicated_data(self, data):
        """
        complicated_data(data)

        data: expected to be a dictionary containing the following:

        'the_strings': a list of strings, to be concatenated

        'the_ints': a list of ints, to be summed, and to have each
        element multiplied by two.

        the return will be a list: first element, the concatenated
        strings.  second element, the sum; third element, the list of
        ints multiplied by two.

        """

        print data
        retval = []
        retval.append(''.join(data["the_strings"]))
        retval.append(sum(data["the_ints"]))
        retval.append(map(lambda x: x + 2, data["the_ints"]))
        print retval
        return retval


def signal_handler(signal, frame):
    """
    Called when program interrupted by SIGINT
    """
    global proxy
    proxy.quit_loop()


proxy = None

# The proxy server, can proxy many classes.


def main_loop():
    """Runs the server. This test uses PPPProxyServer, which is a worker
    proxy in a Paranoid Pirate Protocol configuration. In Paranoid
    Pirate, workers and clients are both ZMQ clients, and connect to a
    broker. The broker and workers heartbeat each other, so
    reliability and error reporting is high.

    """

    global proxy

    ctx = zmq.Context()

    fail = True

    while fail:
        try:
            url = "tcp://ajax:5556"
            proxy = PPPProxyServer('PLAYER1', ctx, url)
            fail = False
        except ZMQError:
            pass

    # A class to expose
    animals = Animals()
    interesting = MoreInteresting()
    # Expose some interfaces. The classes can be any class, including
    # contained within another exposed class. The name can be anything
    # at all that uniquely identifies the interface.
    proxy.expose("animals", animals)
    proxy.expose("animals.angry", animals.angry)
    proxy.expose("interesting", interesting)

    # Run the proxy:
    proxy.run_loop()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    signal.signal(signal.SIGINT, signal_handler)
    main_loop()
