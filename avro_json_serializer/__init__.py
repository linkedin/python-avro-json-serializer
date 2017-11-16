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
    basestring = str

try:
    from collections import OrderedDict
except ImportError:
    # In Python version older than 2.7 use simplejson,
    # as it is already required by avro.
    from simplejson.ordered_dict import OrderedDict


class AvroJsonBase(object):
    """
    Base class for both serializer and deserializer classes.
    """

    """
    This charset will be used to encode binary data for `fixed` and `bytes` types
    """
    BYTES_CHARSET = "ISO-8859-1"

    """
    Charset for JSON. Python uses "utf-8"
    """
    JSON_CHARSET = "utf-8"

    """
    No need to serialize primitives
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

    def __init__(self, avro_schema):
        """
        :param avro_schema: instance of `avro.schema.Schema`
        """
        self._avro_schema = avro_schema
        self.COMPLEX_CONVERTERS = {
            "null": self._process_null,
            "array": self._process_array,
            "map": self._process_map,
        }

    def _process_null(self, *args):
        """
        Always produce None, which will be (de)serialized into "null" in json.
        """
        return None

    def _process_array(self, schema, datum):
        """
        Array is (de)serialized into array.
        Every element is processed recursively according to `items` schema.
        :param schema: Avro schema of `datum`
        :param datum: Data to serialize
        :return: serialized array (list)
        """
        if datum is None:
            raise AvroTypeException(schema, datum)
        process = functools.partial(self._process_data, schema.items)
        return list(map(process, datum))

    def _process_map(self, schema, datum):
        """
        Map is serialized into a map.
        Every value is serialized recursively according to `values` schema.
        :param schema: Avro schema of `datum`
        :param datum: Data to serialize.
        :return: map with serialized values
        """
        if datum is None:
            raise AvroTypeException(schema, datum)
        process = functools.partial(self._process_data, schema.values)
        return dict((key, process(value)) for key, value in six.iteritems(datum))

    def _union_name(self, schema):
        """
        Produce the type name for a union of the given schema.
        :param schema: Avro schema for a value
        :return: string containing the schema type or name
        """
        name = schema.type
        if isinstance(schema, avro.schema.NamedSchema):
            if schema.namespace:
                name = schema.fullname
            else:
                name = schema.name
        return name

    def _validate(self, schema, datum):
        """
        Validate a datum matches a schema.
        :param schema: Avro schema to match against the `datum`
        :param datum: Data to validate
        """
        return validate(schema, datum)

    def _process_data(self, schema, datum):
        """
        Non-specific serialize function.
        It checks type in the schema and calls correct (de)serialization.
        :param schema: Avro schema of the `datum`
        :param datum: Data to process
        """
        if not self._validate(schema, datum):
            raise AvroTypeException(schema, datum)

        if schema.type in self.PRIMITIVE_CONVERTERS:
            return datum

        if schema.type in self.COMPLEX_CONVERTERS:
            return self.COMPLEX_CONVERTERS[schema.type](schema, datum)

        raise avro.schema.AvroException("Unknown type: %s" % schema.type)


class AvroJsonSerializer(AvroJsonBase):
    """
    Use this class for avro json serialization:

        serializer = AvroJsonSerializer(avro_schema)
        serializer.to_json(data)

    """

    def __init__(self, avro_schema):
        """
        :param avro_schema: instance of `avro.schema.Schema`
        """
        super(AvroJsonSerializer, self).__init__(avro_schema)
        self.COMPLEX_CONVERTERS.update({
            "union": self._serialize_union,
            "error_union": self._serialize_union,
            "record": self._serialize_record,
            "request": self._serialize_record,
            "error": self._serialize_record,
            "fixed": self._serialize_binary_string,
            "bytes": self._serialize_binary_string
        })

    def _serialize_binary_string(self, schema, datum):
        """
        The `fixed` and `bytes` datum  are serialized as "ISO-8859-1", but we
        need to re-encode it to UTF-8 for JSON in Python 2.
        """
        string = datum.decode(self.BYTES_CHARSET)
        if six.PY2:
            string = string.encode(self.JSON_CHARSET)
        return string

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
        for candidate_schema in schema.schemas:
            if validate(candidate_schema, datum):
                if candidate_schema.type == "null":
                    return self._process_null()
                else:
                    field_type_name = self._union_name(candidate_schema)
                    return {
                        field_type_name: self._process_data(candidate_schema, datum)
                    }
        raise AvroTypeException(schema, datum)

    def _serialize_record(self, schema, datum):
        """
        Records are serialized into ordered dicts in the order of fields in the schema.
        Every field value is serialized based on it's schema.
        :param schema: Avro schema of this record
        :param datum: Data to serialize
        """
        result = OrderedDict()
        for field in schema.fields:
            result[field.name] = self._process_data(field.type, datum.get(field.name))
        return result

    def to_ordered_dict(self, datum):
        return self._process_data(self._avro_schema, datum)

    def to_json(self, datum):
        result = self.to_ordered_dict(datum)
        # we use separators to minimize size of the output string
        return json.dumps(result, separators=(",", ":"))


class AvroJsonDeserializer(AvroJsonBase):
    """
    Use this class for avro json deserialization:

        deserializer = AvroJsonDeserializer(avro_schema)
        deserializer.from_json(data)

    """

    """Internally used to distinguish from "null" values on dict.get."""
    class UnsetValue(object):
        pass
    UNSET = UnsetValue()

    def __init__(self, avro_schema):
        """
        :param avro_schema: instance of `avro.schema.Schema`
        """
        super(AvroJsonDeserializer, self).__init__(avro_schema)
        self.COMPLEX_CONVERTERS.update({
            "union": self._deserialize_union,
            "error_union": self._deserialize_union,
            "record": self._deserialize_record,
            "request": self._deserialize_record,
            "error": self._deserialize_record,
            "fixed": self._deserialize_binary_string,
            "bytes": self._deserialize_binary_string
        })

    def _deserialize_binary_string(self, schema, datum):
        """
        `fixed` and `bytes` datum are serialized as "ISO-8859-1".
        """
        # assume byte types are already in ISO-8859-1
        if isinstance(datum, bytes):
            return datum

        # otherwise assume unicode string that must be encoded
        return datum.encode(self.BYTES_CHARSET)

    def _deserialize_union(self, schema, datum):
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
        for candidate_schema in schema.schemas:
            if self._validate_union(candidate_schema, datum):
                if candidate_schema.type == "null":
                    return self._process_null()
                else:
                    field_type_name = self._union_name(candidate_schema)
                    return datum[field_type_name]
        raise AvroTypeException(schema, datum)

    def _deserialize_record(self, schema, datum):
        """
        Records are serialized into ordered dicts in the order of fields in the schema.
        Every field value is serialized based on it's schema.
        :param schema: Avro schema of this record
        :param datum: Data to serialize
        """
        result = OrderedDict()
        for field in schema.fields:
            result[field.name] = self._process_data(field.type, datum.get(field.name, self.UNSET))
        return result

    def _validate(self, schema, datum):
        """
        Validate a datum matches a schema.
        :param schema: Avro schema to match against the `datum`
        :param datum: Data to validate
        """
        if datum == self.UNSET:
            return False

        schema_type = schema.type

        # Deserialized as unicode, convert to str for avro validation
        if schema_type in ['fixed', 'bytes']:
            datum = self._deserialize_binary_string(schema, datum)

        # From the `avro.io.Validate` function in avro-python3.
        # Recursive calls replaced so missing field values and binary fields in containers
        # are handled properly (see self.UNSET and above binary handling).
        if schema_type == 'array':
            return (isinstance(datum, list) and
                    all(self._validate(schema.items, d) for d in datum))
        elif schema_type == 'map':
            return (isinstance(datum, dict) and
                    all(isinstance(k, basestring) for k in datum.keys()) and
                    all(self._validate(schema.values, v) for v in datum.values()))
        elif schema_type in ['union', 'error_union']:
            return any(self._validate_union(s, datum) for s in schema.schemas)
        elif schema_type in ['record', 'error', 'request']:
            return (isinstance(datum, dict) and
                    all(self._validate(f.type, datum.get(f.name, self.UNSET))
                        for f in schema.fields))

        return validate(schema, datum)

    def _validate_union(self, schema, datum):
        """
        Validate a wrapped union value, which might be None.
        :param schema: Avro schema of the `datum`
        :param datum: dict deserialized from json
        """
        if datum == self.UNSET:
            return False

        wrapped_datum = datum
        name = self._union_name(schema)
        if datum and name in datum:
            wrapped_datum = datum[name]
        return self._validate(schema, wrapped_datum)

    def from_dict(self, datum):
        """
        Deserialize an Avro dict that was deserialized by the json library.
        :param schema: Avro schema of the `datum`
        :param datum: validated python dict object
        """
        # asssume data has been deserialized already
        return self._process_data(self._avro_schema, datum)

    def from_json(self, datum):
        """
        Deserialize an Avro json string.
        :param schema: Avro schema of the `datum`
        :param datum: string of serialized
        """
        # json library does the bulk of the work
        return self.from_dict(json.loads(datum))
