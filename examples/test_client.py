import zmq
from ZMQJSONProxy import ZMQJSONProxyClient
ctx = zmq.Context()

URL = ""

ap=ZMQJSONProxyClient(ctx, 'animals', URL)
ip = ZMQJSONProxyClient(ctx, 'interesting', URL)
ap.angry = ZMQJSONProxyClient(ctx, 'animals.angry', URL)

my_dict = {'the_strings': ['foo', 'bar', 'baz'], 'the_ints': [i for i in range(10)]}

my_bad_dict = {'the_string': ['foo', 'bar', 'baz'], 'the_ints': [i for i in range(10)]}
