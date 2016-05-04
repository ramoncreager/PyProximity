import zmq
from PyProximity import REQREPProxyClient as ProxyClient
ctx = zmq.Context()

URL = "tcp://ajax:56870"

ap = ProxyClient('PLAYER1', ctx, 'animals', URL)
ip = ProxyClient('PLAYER1', ctx, 'interesting', URL)
ap.angry = ProxyClient('PLAYER1', ctx, 'animals.angry', URL)
my_dict = {'the_strings': ['foo', 'bar', 'baz'],
           'the_ints': [i for i in range(10)]}

my_bad_dict = {'the_string': ['foo', 'bar', 'baz'],
               'the_ints': [i for i in range(10)]}
