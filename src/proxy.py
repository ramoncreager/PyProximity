######################################################################
#
#  simple_proxy.py -- Implements simple proxy classes to enable an
#  object to be proxied via ZMQ and JSON, with a direct, simple
#  REQ/REP pattern. Currently only methods are proxied. Methods that
#  are exported may be called on the proxy class as if they were
#  local. They even include the original doc strings, and may be
#  called using positional or keyword arguments. Typical use (see unit
#  tests):
#
#  class Foo:
#      def add_two(self, x, y):
#          """
#          Adds two values together.
#          """
#          return x + y
#
#  On server, assuming a zmq.Context() 'ctx' and a well known url:
#
#      proxy = PyProximityServer(ctx, url)
#      foo = Foo()
#      proxy.expose("foo", foo)
#      proxy.run_loop()
#
#  On the client (same assumptions):
#
#      foo_proxy = PyProximityClient(ctx, 'foo', url)
#      ret = foo_proxy.add_two(2, 2) # ret = 4
#      ret = foo_proxy.add_two(2, y = 3) # ret = 5
#      ret = foo_proxy.add_two(y = 3, x = 4) #ret = 7
#      foo_proxy.add_two.__doc__ # returns Foo.add_two's doc string
#
#  Copyright (C) 2013 Associated Universities, Inc. Washington DC, USA.
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
import sys
import traceback
import types
import inspect
import thread
import time
import datetime
import msgpack
import logging as log

from zmq.error import ZMQError
from threading import Thread
from PyProximity import JSONEncoder
from PyProximity import PyProximityException
from PyProximity import PP_VALS as PP
from PyProximity import create_worker

######################################################################
# ProxyServer base class
######################################################################


class ProxyServer(object):
    """The base class for a Proxy server. Proxy servers take a class and
       expose their 'public' (i.e. non underscore) member functions as
       RPC servers. PyProximity provides two different server
       patterns: Simple REQ/REP, and the far more complex Paranoid
       Pirate reliable REQ/REP pattern. For the former, the
       ProxyServer is a server in functionality and in implementation;
       the latter is a server in functionality only, as it is a ZMQ
       client that behaves as a worker that connects to a broker.

    """

    def __init__(self, id, ctx, URL, Encoder=JSONEncoder):
        """
        ctx: A ZeroMQ context (zmq.Context())
        URL: The server's URL. May be any legal 0MQ URL:
             'inproc', 'IPC', 'tcp'.
        Encoder: The seralizer/deserializer. Defaults to JSONEncoder.
                 There is also a MsgPackEncoder available.
        """
        self.id = id
        self.encoder = Encoder()
        self.url = URL
        self.pipe_url = "inproc://ctrl_pipe" + '.' + self.id
        self.interfaces = {}
        self.ctx = ctx
        log.info(URL)

    def _send(self, sock, msg):
        raise PyProximityException("Unimplemented base class function called!")

    def _recv(self, sock):
        raise PyProximityException("Unimplemented base class function called!")

    def expose(self, name, obj):
        """
        expose(name, obj):
          name: The name of the exposed interface, may be anything.
          obj: the object whose functions should be proxied by this class.

        This function collects the named functions of 'obj' and saves
        the names and functions in a dictionary, which itself is an
        element of the 'self.interfaces' dictionary under 'name', thus
        allowing any number of objects to be proxied.
        """

        self.interfaces[name] = \
            {p[0]: p[1] for p
             in inspect.getmembers(obj, predicate=inspect.ismethod)}

    def dispatch(self, message):
        """Given a dictionary 'message', dispatches it, calling the correct
        function.  The message must have the following keys:

        'name'   : The name of the object being proxied.
        'proc'   : The name of the member function for that object
        'args'   : The list of arguments for that member function
        'kwargs' : The list of keyword arguments for that member function.

        """
        try:
            f_dict = self.interfaces[message['name']]
            proc = f_dict[str(message['proc'])]
            args = message['args']
            kwargs = message['kwargs']
            return proc(*args, **kwargs)
        except:
            return {'EXCEPTION': self.formatExceptionInfo(10)}

    def formatExceptionInfo(self, maxTBlevel=5):
        """Obtains information from the last exception thrown and extracts
        the exception name, data and traceback, returning them in a
        tuple (string, string, [string, string, ...]).  The traceback
        is a list which will be 'maxTBlevel' deep.

        """
        cla, exc, trbk = sys.exc_info()
        excName = cla.__name__
        excArgs = exc.__str__()
        excTb = traceback.format_tb(trbk, maxTBlevel)
        return (excName, excArgs, excTb)

    def list_proxied_interfaces(self, name):
        """List all the exported functions of interface 'name', sends them back
        as a list.

        The format of the list is:

        [(fname,docstring), (fname,docstring), ...]

        If the function exported has no doc string, one will be
        provided, which will be in the form 'fname(<arg signature>)'

        """
        try:
            f_dict = self.interfaces[name]

            exported_funcs = []

            for ef in filter(lambda x: x[0] != '_', f_dict.keys()):
                sig_string = 'usage: %s(%s)' % (ef, ', '.join(
                    inspect.getargspec(f_dict[ef]).args[1:]))
                docstring = '%s\n%s' % (
                    sig_string, self._trim(f_dict[ef].__doc__))
                exported_funcs.append((ef, docstring))

            return exported_funcs
        except KeyError, e:

            return ["Interface error", str(e)]

    def run_loop(self, watchdogfn=None):
        '''The main loop that services the proxy.'''
        raise PyProximityException("Unimplemented base class function called!")

    # See
    # http://stackoverflow.com/questions/2504411/proper-indentation-for-python-multiline-strings
    def _trim(self, docstring):
        if not docstring:
            return ''
        # Convert tabs to spaces (following the normal Python rules)
        # and split into a list of lines:
        lines = docstring.expandtabs().splitlines()
        # Determine minimum indentation (first line doesn't count):
        indent = sys.maxint
        for line in lines[1:]:
            stripped = line.lstrip()
            if stripped:
                indent = min(indent, len(line) - len(stripped))
        # Remove indentation (first line is special):
        trimmed = [lines[0].strip()]
        if indent < sys.maxint:
            for line in lines[1:]:
                trimmed.append(line[indent:].rstrip())
        # Strip off trailing and leading blank lines:
        while trimmed and not trimmed[-1]:
            trimmed.pop()
        while trimmed and not trimmed[0]:
            trimmed.pop(0)
        # Return a single string:
        return '\n'.join(trimmed)

######################################################################
# REQREPProxyServer. Based on ProxyServer, proxies an object using a
# simple REQ/REP pattern:
#
#     ProxyClient
#      |    ^
#      |    |
#     ProxyServer
#     (object proxied)
#
# The client makes a REQ directly to the ProxyServer's REP socket. The
# ProxyServer then blocks while the REQ is being processed. When done
# the ProxyServer responds using its REP server socket.
#
# This is the easiest pattern to use, but recovering from errors is
# fairly simplistic, and involves time-outs (which may be set on a
# per-call basis, since the server may legitimately be busy performing
# particular requests). If a time-out occurs, the client must close
# its REQ socket, as these hold state: A request must be followed by a
# reply, and if that reply is not forthcoming, that socket may no
# longer be used to make requests.
######################################################################


class REQREPProxyServer(ProxyServer):
    """A simple REQ/REP proxy class. The server provides the REP socket,
    and one or more client(s) connect."""

    def __init__(self, id, ctx, URL, Encoder=JSONEncoder):
        """
        Initializes the proxy server. Binds the server to a url, but
        does not run the server. Use member 'run_loop()' to do that.

        id : The name of the server ('BANKA', 'PLAYER01', etc.)
        ctx: A ZeroMQ context (zmq.Context())
        URL: The server's URL. May be any legal 0MQ URL:
             'inproc', 'IPC', 'tcp'.
        encoder: The seralizer/deserializer. Defaults to JSONEncoder.
                 There is also a MsgPackEncoder available.
        """
        super(REQREPProxyServer, self).__init__(id, ctx, URL, Encoder)
        self.s = self.ctx.socket(zmq.REP)
        self.pipe = self.ctx.socket(zmq.PULL)
        self.s.bind(URL)
        self.pipe.bind(self.pipe_url)
        self.exit_flag = False

    def __del__(self):
        self.s.close()
        self.pipe.close()

    def _send(self, sock, msg):
        packed = self.encoder.encode(msg)
        return sock.send(packed)

    def _recv(self, sock):
        packed = sock.recv()
        return self.encoder.decode(packed)

    def _recover(self, e):
        error = "Encoder: " + str(e)
        self.s.send(self.encoder.encode(error))

    def run_loop(self, watchdogfn=None):
        """
        Runs the server.  This may be run in the server's main thread,
        or can easily be run in another thread. It sets up a poller that
        consists of 2 sockets: the server socket that processes messages
        from the remote proxy client, and a pipe socket that controls
        this loop. When the pipe receives the 'QUIT' message, the loop
        will exit.
        """
        done = False
        poller = zmq.Poller()
        poller.register(self.s, zmq.POLLIN)
        poller.register(self.pipe, zmq.POLLIN)

        if watchdogfn:
            try:
                thread.start_new_thread(
                    self.generate_watchdog_messages, ("WATCHDOG", 1, ))
            except:
                log.warning("Error: unable to start watchdog thread. "
                            "There will be no watchdog.")

        while not done:
            try:
                socks = dict(poller.poll(120000))

                if socks.get(self.s) == zmq.POLLIN:
                    message = self._recv(self.s)

                    if message['proc'] == 'list_methods':
                        methods = self.list_proxied_interfaces(
                            message['name'])
                        self._send(self.s, methods)
                    else:
                        ret_msg = self.dispatch(message)
                        self._send(self.s, ret_msg)

                if socks.get(self.pipe) == zmq.POLLIN:
                    message = self._recv(self.pipe)

                    if message == "QUIT":
                        done = True

                    if message == "WATCHDOG":
                        # This message should never come if watchdogfn
                        # is None, but check to make sure anyway.
                        if watchdogfn:
                            watchdogfn()

            except ZMQError as e:
                err = str(e)
                log.exception(err)

                # could be a CTRL-C, requesting termination
                if "Interrupted system call" not in err:
                    pass  # Hmm, should do something on ZMQ error

            except PyProximityException as e:
                self._recover(e)

        self.exit_flag = True

    def quit_loop(self):
        """
        Tells the main loop 'run_loop()' to exit by passing it a message
        on its control pipe socket.
        """
        pc = self.ctx.socket(zmq.PUSH)
        pc.connect(self.pipe_url)
        packed = self.encoder.encode('QUIT')
        pc.send(packed)

    def generate_watchdog_messages(self, name, delay):
        """Runs as a separate thread, generates 'WATCHDOG' messages for the
        main loop.

        """
        pc = self.ctx.socket(zmq.PUSH)
        pc.connect(self.pipe_url)

        while not self.exit_flag:
            packed = self.encoder.encode(name)
            pc.send(packed)
            time.sleep(delay)

######################################################################
# PPPProxyServer. Based on ProxyServer, this is designed to work with
# a PyProximity Paranoid Pirate Protocol worker, which runs in a
# backgroud thread, communicating with a broker. In this pattern only
# the broker needs to be a well known server. Both the client and the
# worker initiate connections to the broker. This pattern is designed
# for maximum reliability. If a worker dies, the broker will know
# within seconds; and if a request is subsequently made to that
# worker, the broker can immediately and positively tell the client
# that the worker does not exist.
#
# Like the much simpler REQREPProxyServer, this class proxies any
# Python class object, which does the actual work.
#
# The workflow is as follows:
#
#     Proxy Client
#        |  ^
#        |  |
#       broker
#         |  ^
#         |  |
#        PPPworker
#          |  ^
#          |  |
#        ProxyServer
#        (object proxied)
#
# The client makes a request to the broker, which passeds it along to
# the named PPPWorker. This in turn makes a request to the ProxyServer
# (via PUSH socket), which then runs a method on the object being
# proxied. The ProxyServer then takes the result from the object being
# proxied, and passes it back to the PPPWorker via another PUSH
# socket. In this manner the worker is never blocked by the proxied
# object and is able to maintain the reliability protocol with the
# broker as the proxied object does the actual work.
######################################################################


class PPPProxyServer(ProxyServer):
    """An advanced Paranoid Pirate Protocol REQ/REP proxy class. The
    'server' is really a client to a broker (required), which passes
    messages intended for it over a DEALER socket maintained by a
    PyProximity.worker. This class runs the worker in a separate
    thread, listens to requests from the worker via a PULL socket, and
    sends back the answers via a PUSH socket. When it terminates, it
    notifies the worker via the router that it should terminate.

    This arrangement means that the worker thread is never blocked,
    and is thus capable of maintaining a tight heartbeat protocol with
    the broker.

    """

    def __init__(self, id, ctx, URL, Encoder=JSONEncoder):
        """
        Initializes the proxy server. Binds the server to a url, but
        does not run the server. Use member 'run_loop()' to do that.

        ctx: A ZeroMQ context (zmq.Context())
        URL: The server's URL. May be any legal 0MQ URL:
             'inproc', 'IPC', 'tcp'.
        encoder: The seralizer/deserializer. Defaults to JSONEncoder.
                 There is also a MsgPackEncoder available.
        """
        super(PPPProxyServer, self).__init__(id, ctx, URL, Encoder)
        self.worker_request = "ipc:///tmp/worker_request_" + self.id
        self.worker_reply = "ipc:///tmp/worker_reply_" + self.id
        self.ctrl_url = "ipc:///tmp/worker_ctl_" + self.id
        self.exit_flag = False
        self._sock_type = zmq.DEALER

    def run_loop(self, watchdogfn=None):
        """
        Runs the server.  This may be run in the server's main thread,
        or can easily be run in another thread. It sets up a poller that
        consists of 2 sockets: the server socket that processes messages
        from the remote proxy client, and a pipe socket that controls
        this loop. When the pipe receives the 'QUIT' message, the loop
        will exit.
        """
        done = False

        try:
            worker = create_worker(self.id,
                                   self.worker_request,
                                   self.worker_reply,
                                   self.ctrl_url)

            Thread(target=worker).start()
            in_chan = self.ctx.socket(zmq.PULL)
            out_chan = self.ctx.socket(zmq.PUSH)
            pipe = self.ctx.socket(zmq.PULL)
            poller = zmq.Poller()
            poller.register(in_chan, zmq.POLLIN)
            poller.register(pipe, zmq.POLLIN)
            in_chan.bind(self.worker_request)
            pipe.bind(self.pipe_url)
            out_chan.connect(self.worker_reply)

            while not done:
                try:
                    socks = dict(poller.poll(1000))
                    # request from worker
                    if socks.get(in_chan) == zmq.POLLIN:
                        frames = in_chan.recv_multipart()
                        log.debug("Proxy got message: %s", frames)
                        message = self.encoder.decode(frames.pop())
                        # for router, tell it that this is an  RPC response.
                        frames.append(PP.RPC)

                        if message['proc'] == 'list_methods':
                            methods = self.list_proxied_interfaces(
                                message['name'])
                            packed = self.encoder.encode(methods)
                            frames.append(packed)
                            out_chan.send_multipart(frames)
                        else:
                            ret_msg = self.dispatch(message)
                            message['return'] = ret_msg
                            packed = self.encoder.encode(message)
                            frames.append(packed)
                            out_chan.send_multipart(frames)

                    if socks.get(pipe) == zmq.POLLIN:
                        message = pipe.recv()

                        if message == PP.QUIT:
                            log.info("ProxyServer terminating.")
                            wctl = self.ctx.socket(zmq.REQ)
                            wctl.connect(self.ctrl_url)
                            wctl.send(message)
                            rep = wctl.recv()

                            if rep == PP.QUIT:
                                log.info(" Worker %s has terminated.", self.id)

                            done = True
                except msgpack.exceptions.ExtraData as e:
                    log.exception(e)
                except:
                    log.error("Unknown exception in proxy.py")

        except ZMQError as e:
            log.exception(e)

            # could be a CTRL-C, requesting termination
            if "Interrupted system call" not in err:
                pass  # Hmm, should do something on ZMQ error

        except PyProximityException as e:
            log.exception(e)

        self.exit_flag = True

    def quit_loop(self):
        """
        Tells the main loop 'run_loop()' to exit by passing it a message
        on its control pipe socket.
        """
        log.debug("Proxy quit_loop() called")
        pc = self.ctx.socket(zmq.PUSH)
        pc.connect(self.pipe_url)
        pc.send(PP.QUIT)


######################################################################
# ProxyClient. A base class that enables the use of a proxy object as
# if it were a local object. There are currently two alternatives: A
# proxy client that uses the REQ/REP pattern, and a proxy client that
# goes through a router.
######################################################################


class ProxyClient(object):
    """A proxy class to proxy remote objects over a ZMQ
    connection. Currently only proxies member functions that belong to
    'object'.

    The data protocol used is JSON by default, though MsgPack is also
    provided as an option. Custom protocols may be used if the
    PyProximity.Encoder class is subclassed to do so.

    """

    def __init__(self, id, ctx, obj_name, url,
                 Encoder=JSONEncoder, time_out=None):
        """
        Initializes a proxy client.

        ctx      : The 0MQ context (zmq.Context())
        obj_name : The name of the object exposed on the server
        url      : The server's url
        encoder  : the serializer/deserializer to use. Defaults to JSONEncoder;
                   a MsgPackEncoder is also provided. Should match server.
        time_out : Client time-out waiting for server reply, in seconds.
        """
        self._id = id
        self._ctx = ctx
        self._obj_name = obj_name
        self._encoder = Encoder()
        self._time_out = (time_out if time_out else 60) * 1000
        self._initialized = False
        self._proxy_methods = []
        self._url = url

    # __getattr__() will be called when an attribute failure is
    # encountered, as in a function is called that doesn't exist. In
    # that case check to see if the server has returned with the list of
    # functions, and if so finish the initialization.
    def __getattr__(self, name):
        if not self._initialized:
            self._connect_and_register(2.0)
            self._finish_init(1.0)

        if hasattr(self, name):
            return self.__dict__[name]
        else:
            raise AttributeError(name)

    def _send(self, msg):
        """Send method. Extend for specific subclass.

        """
        raise PyProximityException(
            "base class _send(self, sock) is not implemented")

    def _recv(self):
        """Receive  method. Extend for specific subclass.

        """
        raise PyProximityException(
            "base class _recv(self, sock) is not implemented")

    def _cleanup(self):
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.close()

    def _set_request_reply_timeout(self, timeout):
        """
        Sets the time that a request will wait for a reply.

        timeout:
        timeout value, in seconds
        """
        if type(timeout) == datetime.timedelta:
            to = timeout.seconds
        else:
            to = timeout

        self._time_out = to * 1000

    def _get_request_reply_timeout(self):
        """
        Gets the time that a request will wait for a reply, in seconds.
        """
        return self._time_out / 1000

    def _connect_and_register(self, sock_type):
        """
        Attempts to connect to server and requests served functions.
        """
        self._sock = self._ctx.socket(sock_type)
        self._poller = zmq.Poller()
        self._poller.register(self._sock, zmq.POLLIN)
        self._sock.connect(self._url)
        methods = self._get_server_methods(2.0)
        log.debug(methods)

        if methods:
            self._add_methods(methods)

    def _get_server_methods(self, time_out):
        """Requests the list of methods from the server, and returns them to
        the caller.

        """
        raise PyProximityException(
            "ProxyClient._get_server_methods not implemented in base class.")

    def _remove_methods(self):
        """Removes the registered methods

        """
        self._initialized = False

        for m in self._proxy_methods:
            self.__dict__.pop(m)

        self._proxy_methods = []

    def _add_methods(self, methods):
        """Give a list of server methods 'methods', creates local proxy
        methods to correspond.

        """
        for m, d in methods:
            self._add_method(m, d)

        self._initialized = True

    def _add_method(self, method_name, doc_string):
        """
        Adds a proxy method to the client that will behave just as the
        corresponding method on the server does.
        """
        method = types.MethodType(self._generate_method(method_name), self)
        method.__func__.__doc__ = doc_string
        self.__dict__[method_name] = method
        self._proxy_methods.append(method_name)

    def _generate_method(self, name):
        """
        Creates a closure that will make a remote call to the
        appropriate remote method.
        """

        def new_method(self, *args, **kwargs):
            return self._do_the_deed(name, *args, **kwargs)
        return new_method

    def _do_the_deed(self, *args, **kwargs):
        """
        This method handles the nuts and bolts of calling the remote
        function. It does this by constructing a dictionary that the
        remote server can use to call the correct function and pass on
        the arguments to it, and handles the return value or exception
        information.
        """
        pass

######################################################################
# REQREPProxyClient. A ProxyClient that relies on a simple REQ/REP
# pattern to operate. A measure of reliability is achieved by ensuring
# that any request is accompanied by a timeout. If a timeout is
# encountered then the recovery involves closing the REQ socket,
# opening a new one, and trying again.
######################################################################


class REQREPProxyClient(ProxyClient):
    """
    Simple REQ/REP proxy client.
    """

    def __init__(self, id, ctx, obj_name, url,
                 Encoder=JSONEncoder, time_out=None):
        """
        Initializes a proxy client.

        id       : The name of the server ('BANKA', 'PLAYER01', etc.)
        ctx      : The 0MQ context (zmq.Context())
        obj_name : The name of the object exposed on the server
        url      : The server's url
        encoder  : the serializer/deserializer to use. Defaults to JSONEncoder;
                   a MsgPackEncoder is also provided. Should match server.
        time_out : Client time-out waiting for server reply, in seconds.
        """
        super(REQREPProxyClient, self).__init__(id, ctx, obj_name,
                                                url, Encoder, time_out)
        self._connect_and_register(zmq.REQ)

    def _send(self, msg):
        """Send method. Extend for specific subclass.

        """
        packed = self._encoder.encode(msg)
        self._sock.send(packed)

    def _recv(self):
        """Receive  method. Extend for specific subclass.

        """
        packed = self._sock.recv()
        return self._encoder.encode(packed)

    def _get_server_methods(self, time_out):
        """Requests the list of methods from the server, and returns them to
        the caller.

        """
        methods = None

        self._send({'name': self._obj_name,
                    'proc': 'list_methods',
                    'args': [],
                    'kwargs': {}})
        socks = dict(self._poller.poll(time_out * 1000))

        if socks.get(self._sock) == zmq.POLLIN:
            methods = self._recv()

            if "Interface error" in methods:
                raise PyProximityException(
                    methods[0] + methods[1])
        else:
            raise PyProximityException("ProxyClient: %s" % "No server.")

        return methods

    def _do_the_deed(self, *args, **kwargs):
        """
        This method handles the nuts and bolts of calling the remote
        function. It does this by constructing a dictionary that the
        remote server can use to call the correct function and pass on
        the arguments to it, and handles the return value or exception
        information.
        """
        if not self._initialized:
            self._finish_init()

        msg = {'name': self._obj_name, 'proc': args[0],
               'args': args[1:], 'kwargs': kwargs}

        try:
            self._send(msg)
        except ZMQError:
            self._cleanup()
            self._connect_and_register()
            return None

        socks = dict(self._poller.poll(self._time_out))

        if self._sock in socks and socks[self._sock] == zmq.POLLIN:
            reply = self._recv()

            if type(reply) == dict and 'EXCEPTION' in reply:
                raise PyProximityException(reply['EXCEPTION'])

            return reply
        else:
            log.warning("socket timed out! Check server at %s", self._url)
            self._cleanup()
            self._connect_and_register()
            return None

######################################################################
# PPPProxyClient. A ProxyClient that relies on the use of a broker to
# reach the desired worker. Behaves exactly as the simpler
# REQREPProxyClient does, with the exception of error recovery. This
# proxy client does not rely on timeouts to detect a problem, since it
# will be told immediately by the broker if a worker no longer
# exists. Further, it is up to the worker and router to reestablish
# that connection. The client does not need to do anything.
######################################################################


class PPPProxyClient(ProxyClient):
    """
    A proxy class that operates via a broker.
    """

    def __init__(self, id, ctx, obj_name, url,
                 Encoder=JSONEncoder, time_out=None):
        """
        Initializes a proxy client.

        id       : The name of the server ('BANKA', 'PLAYER01', etc.)
        ctx      : The 0MQ context (zmq.Context())
        obj_name : The name of the object exposed on the server
        url      : The broker's url
        encoder  : the serializer/deserializer to use. Defaults to JSONEncoder;
                   a MsgPackEncoder is also provided. Should match server.
        time_out : Client time-out waiting for server reply, in seconds.
        """
        super(PPPProxyClient, self).__init__(id, ctx, obj_name,
                                             url, Encoder, time_out)
        self._connect_and_register(zmq.DEALER)

    def _send(self, msg):
        """Send method. 'msg' is considered the payload. This method encode
        this with the Encoder policy class, and create the appropriate
        multi-frame message [id, payload].

        """
        packed = self._encoder.encode(msg)
        self._sock.send_multipart([self._id, packed])

    def _recv(self):
        """Receive method. The proxy client may receive one of two kinds of
        messages:

        1 frame: Replies from the router, such as REQ_ACK or NO_SUCH_WORKER.
        2 frames: reply from worker passed on via router.

        In the case of the latter this function will handle the
        decoding of the payload, and return the two frames:

        [id, decoded_payload]

        """
        msg = self._sock.recv_multipart()

        if len(msg) > 1:
            packed = msg.pop()
            unpacked = self._encoder.decode(packed)
            msg.append(unpacked)

        return msg

    def _get_server_methods(self, time_out=2.0):
        """Tries to obtain the server methods. In this case (PPPProxyClient)
        the broker responds first, so we ave the opportunity to learn
        from the brocker if our worker is available.

        """
        methods = None

        self._send({'name': self._obj_name,
                    'proc': 'list_methods',
                    'args': [],
                    'kwargs': {}})

        while True:
            socks = dict(self._poller.poll(time_out * 1000))

            if socks.get(self._sock) == zmq.POLLIN:
                frames = self._recv()
                log.debug("frame received of length %i: %s",
                          len(frames), str(frames))

                if len(frames) == 1:
                    if PP.NO_SUCH_WORKER in frames:
                        raise PyProximityException(
                            "%s: Router @ %s reports 'NO_SUCH_WORKER'"
                            % (self._id, self._url))
                    elif PP.REQ_ACK in frames:
                        continue
                else:
                    methods = frames.pop()

                    if 'Interface error' in methods:
                        raise PyProximityException(
                            methods[0] + " " + str(methods[1]))
                    break
            else:
                raise PyProximityException(
                    "%s: No acknowledgement from router @ %s."
                    % (self._id, self._url))

        return methods

    def _do_the_deed(self, *args, **kwargs):
        """
        This method handles the nuts and bolts of calling the remote
        function. It does this by constructing a dictionary that the
        remote server can use to call the correct function and pass on
        the arguments to it, and handles the return value or exception
        information.
        """
        if not self._initialized:
            raise PyProximityException('Client %s is not initialized!'
                                       % self._id)

        msg = {'name': self._obj_name, 'proc': args[0],
               'args': args[1:], 'kwargs': kwargs}

        self._send(msg)
        waited = 0
        ack = False

        while True:
            socks = dict(self._poller.poll(1000))

            if socks.get(self._sock) == zmq.POLLIN:
                frames = self._recv()
                log.debug("frame received of length %i: %s",
                          len(frames), str(frames))

                if len(frames) == 1:
                    if PP.REQ_ACK in frames:
                        # router says all is well, wait for reply.
                        log.debug("all is well, waiting for answer")
                        ack = True
                        continue
                    elif PP.NO_SUCH_WORKER in frames:
                        # if worker went away, must reload
                        # attributes. They may have changed.
                        raise PyProximityException(
                            "%s: Router @ %s reports 'NO_SUCH_WORKER'"
                            % (self._id, self._url))
                else:
                    reply = frames.pop()

                    if type(reply) == dict and 'EXCEPTION' in reply:
                        if 'KeyError' in reply['EXCEPTION']:
                            # check to see if we're up-to-date with
                            # the server. If attribute exists here but
                            # not on server, refetch and sync.
                            methods = self._get_server_methods(2.0)
                            if args[0] in [m for m, _ in methods]:
                                self._remove_methods()
                                self._add_methods(methods)
                        raise PyProximityException(reply['EXCEPTION'])

                    # confirm that this is the expected result
                    if reply['name'] == msg['name'] \
                       and reply['proc'] == msg['proc']:
                        return reply.pop('return')
                    else:
                        raise PyProximityException(
                            "%s: Mismatched response. Router @ %s\n"
                            "sent: %s\nreceived: %s"
                            % (self._id, self._url,  str(msg), str(reply)))
            else:
                waited += 1000

                if not ack or waited > self._time_out:
                    raise PyProximityException(
                        "%s: socket timed out! Check router at %s"
                        % (self._id, self._url))
        else:
            raise PyProximityException(
                "%s: No acknowledgement from router @ %s."
                % (self._id, self._url))
