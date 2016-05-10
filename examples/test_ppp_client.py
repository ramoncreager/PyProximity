import zmq
from PyProximity import PPPProxyClient as ProxyClient
ctx = zmq.Context()

URL = "tcp://ajax:5555"

ap = ProxyClient('PLAYER1', ctx, 'animals', URL)
ip = ProxyClient('PLAYER1', ctx, 'interesting', URL)
ap.angry = ProxyClient('PLAYER1', ctx, 'animals.angry', URL)
my_dict = {'the_strings': ['foo', 'bar', 'baz'],
           'the_ints': [i for i in range(10)]}

my_bad_dict = {'the_string': ['foo', 'bar', 'baz'],
               'the_ints': [i for i in range(10)]}


def test():
    for i in range(100):
        print ap.cat()
        print ap.dog()
        print ap.frog()
        print ap.angry.cat()
        print ap.angry.dog()
        print ip.add_two(50, 25)
        print ip.div_two(50, 25)
        print ip.sub_two(50, 25)
        print ip.complicated_data(my_dict)
        print ip.long_delay(2)
