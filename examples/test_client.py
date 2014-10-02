import zmq
from py_proximity import PyProximityClient
ctx = zmq.Context()

URL = ""

ap=PyProximityClient(ctx, 'animals', URL)
ip = PyProximityClient(ctx, 'interesting', URL)
ap.angry = PyProximityClient(ctx, 'animals.angry', URL)

my_dict = {'the_strings': ['foo', 'bar', 'baz'], 'the_ints': [i for i in range(10)]}

my_bad_dict = {'the_string': ['foo', 'bar', 'baz'], 'the_ints': [i for i in range(10)]}
