# -*- coding: utf-8 -*-

# (c) [2014] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""
Simple module that allows to serialize avro data into OrderedDict(from simplejson) or to json string.

Inspired by avro.io.DatumWriter (which writes binary avro)
"""

import functools
import json

import avro.schema
from avro.io import AvroTypeException
import six

if six.PY2:
    from avro.io import validate
else:
    from avro.io import Validate as validate

try:
    from collections import OrderedDict
except ImportError:
    # In Python version older than 2.7 use simplejson,
    # as it is already required by avro.
    from simplejson.ordered_dict import OrderedDict


class AvroJsonSerializer(object):
    """
    Use this class for avro json serialization:

        serializer = AvroJsonSerializer(avro_schema)
        serializer.to_json(data)

    """

    """
    This charset will be used to encode binary data for `fixed` and `bytes` types
    """
    BYTES_CHARSET = "ISO-8859-1"

    """
    Charset for JSON. Python uses "utf-8"
    """
    JSON_CHARSET = "utf-8"

    def __init__(self, avro_schema):
        """
        :param avro_schema: instance of `avro.schema.Schema`
        """
        self._avro_schema = avro_schema

    def _serialize_binary_string(self, avro_schema, datum):
        """
        `fixed` and `bytes` datum  are serialized as "ISO-8859-1", but we need to re-encode it to UTF-8 for JSON in Python 2.
        """
        string = datum.decode(self.BYTES_CHARSET)
        if six.PY2:
            string = string.encode(self.JSON_CHARSET)
        return string

    def _serialize_null(self, *args):
        """
        Always serialize into None, which will be serialized into "null" in json.
        """
        return None

    def _serialize_array(self, schema, datum):
        """
        Array is serialized into array.
        Every element is serialized recursively according to `items` schema.
        :param schema: Avro schema of `datum`
        :param datum: Data to serialize
        :return: serialized array (list)
        """
        if datum is None:
            raise AvroTypeException(schema, datum)
        serialize = functools.partial(self._serialize_data, schema.items)
        return list(map(serialize, datum))

    def _serialize_map(self, schema, datum):
        """
        Map is serialized into a map.
        Every value is serialized recursively according to `values` schema.
        :param schema: Avro schema of `datum`
        :param datum: Data to serialize.
        :return: map with serialized values
        """
        if datum is None:
            raise AvroTypeException(schema, datum)
        serialize = functools.partial(self._serialize_data, schema.values)
        return dict((key, serialize(value)) for key, value in six.iteritems(datum))

    def _serialize_union(self, schema, datum):
        """
        With union schema has multiple possible schemas.
        We iterate over possible schemas and see which one fits `datum` passed.
        Union serialization:
        if null:
            "null"
        else:
            {"<type>": value}
        Then used one that matches to serialize `datum`
        :param schema: Avro schema for this union
        :param datum: Data to serialize
        :return: dict {"type": value} or "null"
        """
        for candiate_schema in schema.schemas:
            if validate(candiate_schema, datum):
                if candiate_schema.type == "null":
                    return self._serialize_null()
                else:
                    field_type_name = candiate_schema.type
                    if isinstance(candiate_schema, avro.schema.NamedSchema):
                        field_type_name = candiate_schema.name
                    return {
                        field_type_name: self._serialize_data(candiate_schema, datum)
                    }
        raise schema.AvroTypeException(schema, datum)

    def _serialize_record(self, schema, datum):
        """
        Records are serialized into ordered dicts in the order of fields in the schema.
        Every field value is serialized based on it's schema.
        :param schema: Avro schema of this record
        :param datum: Data to serialize
        """
        result = OrderedDict()
        for field in schema.fields:
            result[field.name] = self._serialize_data(field.type, datum.get(field.name))
        return result

    """No need to serialize primitives
    """
    PRIMITIVE_CONVERTERS = frozenset((
        "boolean",
        "string",
        "int",
        "long",
        "float",
        "double",
        "enum"
    ))

    """Some conversions require custom logic so we have separate functions for them.
    """
    COMPLEX_CONVERTERS = {
        "null": _serialize_null,
        "array": _serialize_array,
        "map": _serialize_map,
        "union": _serialize_union,
        "error_union": _serialize_union,
        "record": _serialize_record,
        "request": _serialize_record,
        "error": _serialize_record,
        "fixed": _serialize_binary_string,
        "bytes": _serialize_binary_string
    }

    def _serialize_data(self, schema, datum):
        """
        Non-specific serialize function.
        It checks type in the schema and calls correct serialization.
        :param schema: Avro schema of the `datum`
        :param datum: Data to serialize
        """
        if not validate(schema, datum):
            raise AvroTypeException(schema, datum)

        if schema.type in AvroJsonSerializer.PRIMITIVE_CONVERTERS:
            return datum

        if schema.type in AvroJsonSerializer.COMPLEX_CONVERTERS:
            return self.COMPLEX_CONVERTERS[schema.type](self, schema, datum)

        raise avro.schema.AvroException("Unknown type: %s" % schema.type)

    def to_ordered_dict(self, datum):
        return self._serialize_data(self._avro_schema, datum)

    def to_json(self, datum):
        result = self.to_ordered_dict(datum)
        # we use separators to minimize size of the output string
        return json.dumps(result, separators=(",", ":"))
