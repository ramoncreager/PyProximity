######################################################################
#  pp_encoder.py - A base class for the serializers used in PyProximity
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

import json
import msgpack
import datetime
from PyProximity import PyProximityException


class Encoder(object):

    def __init__(self):
        self.encoders = []
        self.decoders = []

    def add_custom_encoding(self, encoder, decoder):
        self.encoders.append(encoder)
        self.decoders.append(decoder)

    def _decode_custom(self, obj):
        """Add custom code to decode objects here. If not found, 'obj' is
        returned as is.

        """

        o = [False, obj]

        for f in self.decoders:
            o = f(o)

        return o[1]

    def _encode_custom(self, obj):
        """Add custom code to encode objects here. If the object is not found
        the object is returned as is.

        """
        o = [False, obj]

        for f in self.encoders:
            o = f(o)

        return o[1]

    def encode(self, obj):
        """Encodes a Python object 'obj' to its wire format."""
        raise PyProximityException("Base Class 'Encode' cannot be used"
                                   " without subclassing first")

    def decode(self, packed):
        """Decodes an object 'packed' from wire to Python format"""
        raise PyProximityException("Base Class 'Encode' cannot be used"
                                   " without subclassing first")


class JSONEncoder(Encoder):

    def __init__(self):
        super(JSONEncoder, self).__init__()

    def encode(self, obj):
        """Encodes a Python object 'obj' to its JSON format."""

        try:
            packed = json.dumps(obj)
        except TypeError as e:
            raise PyProximityException(str(e))

        return packed

    def decode(self, packed):
        """Decodes a JSON object 'packed' to Python format"""

        try:
            obj = json.loads(packed)
        except ValueError as e:
            raise PyProximityException(str(e))

        return obj


# sample encoder and decoder functions compatible with
# 'add_custom_encoding' and the generic encoding/decoding functions.

def decode_datetime(obj):
    """decode_datetime(obj)

    Decodes datetime and timedelta objects from datetime.

    obj: A list, [handled, obj]. If the bool 'handled' is set,
    the function will ignore the object.
    """
    # if obj[0] is True, then this has already been decoded, don't handle.
    if not obj[0]:
        if b'__datetime__' in obj[1]:
            obj[1] = datetime.datetime.strptime(
                obj[1]["as_str"], "%Y%m%dT%H:%M:%S.%f")
            obj[0] = True
        elif b'__timedelta__' in obj[1]:
            obj[1] = datetime.timedelta(seconds=obj[1]['total_seconds'])
            obj[0] = True

    return obj


def encode_datetime(obj):
    """encode_datetime(obj_tuple)

    Encodes datetime and timedelta from datetime.

    obj: A list, [handled, obj]. If the bool 'handled' is set,
    the function will ignore the object.
    """
    # if obj[0] is True, then this has already been encoded, don't handle
    if not obj[0]:
        if isinstance(obj[1], datetime.datetime):
            obj[1] = {'__datetime__': True, 'as_str': obj[
                1].strftime("%Y%m%dT%H:%M:%S.%f")}
            obj[0] = True
        elif isinstance(obj[1], datetime.timedelta):
            obj[1] = {'__timedelta__': True,
                      'total_seconds': obj[1].total_seconds()}
            obj[0] = True

    return obj


class MsgPackEncoder(Encoder):

    def __init__(self):
        super(MsgPackEncoder, self).__init__()
        self.add_custom_encoding(encode_datetime, decode_datetime)

    def encode(self, obj):
        """Encodes a Python object 'obj' to its MessagePack format."""

        # close over 'self' to provide 'packb' with custom encoders
        def cust_enc(obj):
            return self._encode_custom(obj)

        packed = msgpack.packb(obj, default=cust_enc)
        return packed

    def decode(self, packed):
        """Decodes a JSON object 'packed' to Python format"""

        # close over 'self' to provide 'unpackb' with the custom decoders
        def cust_dec(obj):
            return self._decode_custom(obj)

        unpacked = msgpack.unpackb(packed, object_hook=cust_dec)
        return unpacked
