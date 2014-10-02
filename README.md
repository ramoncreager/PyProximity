PyProximity
===========

Proxy Python objects via ZeroMQ and MessagePack

## Introduction

A library that implements a simple RPC mechanism for Python objects. The interface definition is published using the JSON protocol, and upon connection the client side dynamically builds proxy methods for each of the exported server methods.

Its features are:

  * Lightweight; uses MessagePack over 0MQ
  * very thin client
  * automatic discovery of server interfaces, including exported docstrings
  * easily proxy 3d party classes
  * single server may expose multiple interfaces
  * Because of 0MQ, no server/client start ordering effects. Server may be stopped and restarted, without requiring client to be recreated or restarted.


Example uses are to allow a central script to communicate with and control one or more daemons on separate machines--for example, if the daemons must be on hosts that control a piece of hardware, such as in an embedded application; or to allow users with a very simple Python environment (all that is needed is PyZMQ) to call a server that has a very complex Python environment; etc.

## Example

A simple example will serve to illustrate this library. Supposing, on the server machine, you have this class:

      class Foo:
          def sub_two(self, x, y):
              """
              Subtracts y from x.

              x: first value
              y: second value

              returns: x - y
              """
              print "returning %s - %s = %s" % (str(x), str(y), str(x - y))
              return x - y

          def div_two(self, x, y):
              print "returning %d / %d = %d" % (x, y, x / y)
              return x / y

You wish to call `sub_two` and `div_two` from an entirely separate Python session on perhaps a different host. With the PyProximity you can easily do so. On the server machine, in an interactive Python session, enter the following code (assuming class Foo above is already defined):

      import zmq
      from py_proximity import PyProximityServer

      ctx = zmq.Context()
      url = "tcp://0.0.0.0:5555"
      server = PyProximityServer(ctx, url)
      foo = Foo()
      server.expose("foo", foo)
      server.run_loop()
      
The server is now running on TCP port 5555. Calling the server from a client is straightforward. Let's assume the server is on the host named 'colossus':

      import zmq
      from py_proximity import PyProximityClient
      ctx = zmq.Context()
      URL = "tcp://colossus:5555"
      foo = PyProximityClient(ctx, 'foo', URL)
      
At this point you can examine the proxy class 'foo' and see what's there, using iPython's tab completion for example:

      In [2]: foo.
      foo.add_two  foo.div_two
     
Our three functions are there!

You may examine the docstrings:

      In [2]: foo.add_two?
      Type:       instancemethod
      Base Class: <type 'instancemethod'>
      String Form:<bound method ?.new_method of <py_proximity.PyProximityClient object at 0x1ab1ed0>>
      Namespace:  Interactive
      File:       /home/sandboxes/rcreager/ZMQ/pyzmqproxy/py_proximity.py
      Definition: foo.add_two(self, *args, **kwargs)
      Docstring:
      usage: add_two(x, y)

      Adds two values together.

      x: first value
      y: second value

      returns: x + y

Note that the function signature (Definition:) will not match that on the server, instead giving the one for the proxy method. You should rely on the docstring to convey this information. The server will add a 'usage: foo(bar)' line to the front of the docstring, and the served function's docstring should expand on this. If the exported function has no docstring this will at least convey some useful information.

      In [3]: foo.div_two?
      Type:       instancemethod
      Base Class: <type 'instancemethod'>
      String Form:<bound method ?.new_method of <py_proximity.PyProximityClient object at 0x1ab1ed0>>
      Namespace:  Interactive
      File:       /home/sandboxes/rcreager/ZMQ/pyzmqproxy/py_proximity.py
      Definition: foo.div_two(self, *args, **kwargs)
      Docstring: usage: div_two(x, y)

The functions may be called just as if they were local:

      In [4]: foo.div_two(500, 2)
      Out[4]: 250

So far we've exported a very simple class that we wrote ourselves. But this can work as well with 3d party classes (subject to msgpack encoding restrictions; see section on Limitations). Let's assume a server controlling an FPGA roach via the KATCP library, with a remote client (perhaps on a laptop) which does not have the KATCP library in the Python environment. On the server:

      import zmq
      from py_proximity import PyProximityServer
      from corr import katcp_wrapper

      ctx = zmq.Context()
      url = "tcp://0.0.0.0:5555"
      server = PyProximityServer(ctx, url)
      srbs = katcp_wrapper.FpgaClient("srbsr2-1")
      server.expose("srbs", srbs)
      server.run_loop()

Now, on a different host, in the client:

      import zmq
      from py_proximity import PyProximityClient
      ctx = zmq.Context()
      URL = "tcp://colossus:5555"
      srbs = PyProximityClient(ctx, 'srbs', URL)
      
Using iPython we can see that 'srbs' provides all of the 'public' (non leading underscore)  methods from class `katcp_wrapper.FpgaClient` (even though we have not imported it and may not even have access to it):

      In [6]: srbs.
      srbs.blindwrite                srbs.listcmd                   srbs.snapshot_get
      srbs.blocking_request          srbs.listdev                   srbs.start
      srbs.bulkread                  srbs.notify_connected          srbs.status
      srbs.config_10gbe_core         srbs.ping                      srbs.stop
      srbs.est_brd_clk               srbs.print_10gbe_core_details  srbs.tap_start
      srbs.execcmd                   srbs.progdev                   srbs.tap_stop
      srbs.get_rcs                   srbs.qdr_rst                   srbs.unhandled_inform
      srbs.get_snap                  srbs.qdr_status                srbs.unhandled_reply
      srbs.handle_inform             srbs.read                      srbs.unhandled_request
      srbs.handle_message            srbs.read_dram                 srbs.upload_bof
      srbs.handle_reply              srbs.read_int                  srbs.wait_connected
      srbs.handle_request            srbs.read_uint                 srbs.write
      srbs.inform_log                srbs.request                   srbs.write_dram
      srbs.is_connected              srbs.run                       srbs.write_int
      srbs.join                      srbs.running
      srbs.listbof                   srbs.send_message

and that each of these functions may be explored by looking at their docstring:

      In [5]: srbs.tap_start?
      Type:       instancemethod
      Base Class: <type 'instancemethod'>
      String Form:<bound method ?.new_method of <py_proximity.PyProximityClient object at 0x2a6f4d0>>
      Namespace:  Interactive
      File:       /home/sandboxes/rcreager/ZMQ/pyzmqproxy/py_proximity.py
      Definition: srbs.tap_start(self, *args, **kwargs)
      Docstring:
      usage: tap_start(tap_dev, device, mac, ip, port)
      Program a 10GbE device and start the TAP driver.

      @param self  This object.
      @param device  String: name of the device (as in simulink name).
      @param tap_dev  String: name of the tap device (a Linux identifier). If you want to destroy a device later, you need to use this name.
      @param mac   integer: MAC address, 48 bits.
      @param ip    integer: IP address, 32 bits.
      @param port  integer: port of fabric interface (16 bits).

      Please note that the function definition changed from corr-0.4.0 to corr-0.4.1 to include the tap_dev identifier.

and called as if local:

      In [9]: srbs.listdev()
      Out[9]:               
      [u'adc5g_controller', 
       u'adcsnap0_bram',    
       u'adcsnap0_ctrl',    
       u'adcsnap0_status',  
       u'adcsnap1_bram',    
       u'adcsnap1_ctrl',    
       u'adcsnap1_status',  

       ... # (snip)

       u'ssg_status_out',
       u'status',
       u'sys_board_id',
       u'sys_clkcounter',
       u'sys_rev',
       u'sys_rev_rcs',
       u'sys_scratchpad',
       u'trig',
       u'xblocks_lib']

Just like the real katcp_wrapper.FpgaClient.listdev() on the server, our proxy also returns a list of register names.

Finally, note that PyProximityServer supports exposing multiple interface on one server instance. If one wished to expose `foo` and `srbs` on the same server, just call `expose()` as many times as needed:

      server = PyProximityServer(ctx, url)
      srbs = katcp_wrapper.FpgaClient("srbsr2-1")
      foo = Foo()
      server.expose("foo", foo)
      server.expose("srbs", srbs)
      server.run_loop()

## Exceptions

The class handles exceptions at the server by packaging the remote traceback into a list and returning it to the client. The client will then throw a `PyProximityException` with this list as the message:

      In [13]: foo.div_two(2, 0)
      ---------------------------------------------------------------------------
      PyProximityException                     Traceback (most recent call last)
      /home/sandboxes/rcreager/ZMQ/pyzmqproxy/<ipython-input-13-c4b22450d183> in <module>()
      ----> 1 foo.div_two(2, 0)

      /home/sandboxes/rcreager/ZMQ/pyzmqproxy/py_proximity.pyc in new_method(self, *args, **kwargs)
          369         """
          370         def new_method(self, *args, **kwargs):
      --> 371             return self._do_the_deed(name, *args, **kwargs)
          372         return new_method
          373

      /home/sandboxes/rcreager/ZMQ/pyzmqproxy/py_proximity.pyc in _do_the_deed(self, *args, **kwargs)
          398
          399             if type(repl) == dict and repl.has_key('EXCEPTION'):
      --> 400                 raise PyProximityException(repl['EXCEPTION'])
          401
          402             return repl

      PyProximityException: [u'ZeroDivisionError', u'integer division or modulo by zero', [u'  File "py_proximity.py", line 137, in dispatch\n    return proc(*args, **kwargs)\n', u'  File "<ipython-input-34-96e5d881cb85>", line 17, in div_two\n    print "returning %d / %d = %d" % (x, y, x / y)\n']]

## Installation

Installation is simplistic at this point: Simply place `py_proximity.py` somewhere in your PYTHONPATH. [ZeroMQ](http://zeromq.org), [PyZMQ](https://github.com/zeromq/pyzmq), and [msgpack-python](https://github.com/msgpack/msgpack-python) must be installed on your system.

## Limitations

  * Currently does not export functions whose names have one or more leading underscores.
  * Only class methods are exposed. Class attributes are not.
  * Class methods that take or return Python objects for which there is no msgpack encoding will not work.
  
## To Do

  * Provide the option of exposing class attributes as well as class methods
  * Provide a regular expressions filter when exposing elements
  * Cleaner exception handling
 
