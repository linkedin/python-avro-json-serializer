# -*- coding: utf-8 -*-

# (c) [2014] LinkedIn Corp. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import avro.io
import avro.schema
import six
from unittest import TestCase

if six.PY2:
    from avro.schema import make_avsc_object
else:
    from avro.schema import make_avsc_object
    long = int

from avro_json_serializer import AvroJsonSerializer, AvroJsonDeserializer

class TestAvroJsonSerializer(TestCase):
    FIELD_ENUM = {
        "name": "fruit",
        "type": {
            "name": "Fruit",
            "type": "enum",
            "symbols": [
                "ORANGE",
                "APPLE",
                "PINEAPPLE"
            ]
        }
    }

    FIELD_INT = {
        "name": "fint",
        "type": "int"
    }

    FIELD_LONG = {
        "name": "flong",
        "type": "long"
    }

    FIELD_FLOAT = {
        "name": "ffloat",
        "type": "float"
    }

    FIELD_DOUBLE = {
        "name": "fdouble",
        "type": "double"
    }

    FIELD_STRING = {
        "name": "fstring",
        "type": "string"
    }

    FIELD_ARRAY_INT = {
        "type": {"type": "array", "items": "int"},
        "name": "intarr"
    }

    FIELD_MAP_INT = {
        "type": {"type": "map", "values": "int"},
        "name": "intmap"
    }

    FIELD_FIXED = {
        "type": {
            "name": "fixed_16",
            "size": 16,
            "type": "fixed"
        },
        "size": 16,
        "name": "ffixed"
    }

    FIELD_RECORD = {
        "type": {
            "name": "Rec",
            "fields": [{
                "name": "subfint",
                "type": "int"
            }],
            "type": "record"
        },
        "name": "frec"
    }

    FIELD_UNION_NULL_INT = {
        "name": "funion_null",
        "type": [
            "int",
            "null"
        ]
    }

    FIELD_UNION_RECORDS = {
        "name": "funion_rec",
        "type": [
            {
                "type": "record",
                "name": "rec1",
                "fields": [
                    {
                        "name": "field",
                        "type": "int"
                    }
                ]
            },
            {
                "type": "record",
                "namespace": "example.avro",
                "name": "rec2",
                "fields": [
                    {
                        "name": "field2",
                        "type": "string"
                    }
                ]
            }
        ]
    }

    ALL_FIELDS_SCHEMA = {
        "type": "record",
        "name": "all_field",
        "fields": [
            FIELD_ENUM,
            FIELD_INT,
            FIELD_LONG,
            FIELD_STRING,
            FIELD_FIXED,
            FIELD_RECORD,
            FIELD_UNION_NULL_INT,
            FIELD_FLOAT,
            FIELD_DOUBLE,
            FIELD_ARRAY_INT,
            FIELD_MAP_INT
        ],
        "namespace": "com.some.thing"
    }

    UNION_FIELDS_SCHEMA = {
        "type": "record",
        "name": "unions",
        "fields": [
            FIELD_UNION_NULL_INT
        ]
    }

    UNION_RECORDS_SCHEMA = {
        "type": "record",
        "name": "unions",
        "fields": [
            FIELD_UNION_RECORDS
        ]
    }

    VALID_DATA_ALL_FIELDS = {
        "fruit": "ORANGE",
        "fint": 1,
        "flong": long(1),
        "ffloat": 1.0,
        "fdouble": 2.0,
        "fstring": "hi there",
        "ffixed": b"1234567890123456",
        "frec": {
            "subfint": 2
        },
        "funion_null": None,
        "intarr": [1, 2, 3],
        "intmap": {"one": 1}
    }

    # unions can't be serialized directly; must be in a record
    INDIVIDUALLY_SERIALIZABLE = list(ALL_FIELDS_SCHEMA['fields'])
    INDIVIDUALLY_SERIALIZABLE.remove(FIELD_UNION_NULL_INT)

    def test_all_supported_types(self):
        avro_schema = make_avsc_object(self.ALL_FIELDS_SCHEMA, avro.schema.Names())
        data = self.VALID_DATA_ALL_FIELDS
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)
        self.assertEquals(avro_json, """{"fruit":"ORANGE","fint":1,"flong":1,"fstring":"hi there","ffixed":"1234567890123456","frec":{"subfint":2},"funion_null":null,"ffloat":1.0,"fdouble":2.0,"intarr":[1,2,3],"intmap":{"one":1}}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_individually_allowed_fields_separately(self):
        for field in self.INDIVIDUALLY_SERIALIZABLE:
            # unwrap enum, fixed, array, and map but save the name for value lookup
            name = field['name']
            if isinstance(field['type'], dict):
                field = field['type']
            avro_schema = make_avsc_object(field, avro.schema.Names())
            data = self.VALID_DATA_ALL_FIELDS[name]
            avro_json = AvroJsonSerializer(avro_schema).to_json(data)
            json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
            self.assertEquals(json_data, data)

    def test_fails_validation(self):
        avro_schema = make_avsc_object(self.ALL_FIELDS_SCHEMA, avro.schema.Names())
        data = dict(self.VALID_DATA_ALL_FIELDS)
        data["ffloat"] = "hi"
        serializer = AvroJsonSerializer(avro_schema)
        self.assertRaises(avro.errors.AvroTypeException, serializer.to_json, data)

    def test_union_serialization_null(self):
        avro_schema = make_avsc_object(self.UNION_FIELDS_SCHEMA, avro.schema.Names())
        data = {
            "funion_null": None
        }
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)
        self.assertEquals(avro_json, """{"funion_null":null}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_union_serialization_not_null(self):
        avro_schema = make_avsc_object(self.UNION_FIELDS_SCHEMA, avro.schema.Names())
        data = {
            "funion_null": 1
        }
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)
        self.assertEquals(avro_json, """{"funion_null":{"int":1}}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_union_serialization_invalid(self):
        avro_schema = make_avsc_object(self.UNION_FIELDS_SCHEMA, avro.schema.Names())
        data = {
            "funion_null": "hi"
        }
        serializer = AvroJsonSerializer(avro_schema)
        self.assertRaises(avro.errors.AvroTypeException, serializer.to_json, data)

    def test_records_union(self):
        avro_schema = make_avsc_object(self.UNION_RECORDS_SCHEMA, avro.schema.Names())
        data = {
            "funion_rec": {
                "field": 1
            }
        }
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)
        self.assertEquals(avro_json, """{"funion_rec":{"rec1":{"field":1}}}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

        data_another_record = {
            "funion_rec": {
                "field2": "hi"
            }
        }
        another_record_json = AvroJsonSerializer(avro_schema).to_json(data_another_record)
        self.assertEquals(another_record_json, """{"funion_rec":{"example.avro.rec2":{"field2":"hi"}}}""")
        another_json_data = AvroJsonDeserializer(avro_schema).from_json(another_record_json)
        self.assertEquals(another_json_data, data_another_record)

    def test_map(self):
        schema_dict = {
            "type": "record",
            "name": "rec",
            "fields": [
                self.FIELD_MAP_INT
            ]
        }
        data = {
            "intmap": {
                "one": 1,
                "two": 2
            }
        }
        unicode_dict = {
            'intmap': {
                'one': 1,
                u'two': 2
            }
        }

        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)

        # Dictionaries are unsorted
        self.assertIn(avro_json, ("""{"intmap":{"one":1,"two":2}}""", """{"intmap":{"two":2,"one":1}}"""))

        deserializer = AvroJsonDeserializer(avro_schema)
        json_data = deserializer.from_json(avro_json)
        self.assertEquals(json_data, data)

        mixed_unicode = deserializer.from_dict(unicode_dict)
        self.assertEquals(mixed_unicode, data)

    def test_array(self):
        schema_dict = {
            "type": "record",
            "name": "rec",
            "fields": [
                self.FIELD_ARRAY_INT
            ]
        }
        data = {
            "intarr": [1, 2, 3]
        }
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        avro_json = AvroJsonSerializer(avro_schema).to_json(data)
        self.assertEquals(avro_json, """{"intarr":[1,2,3]}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_user_record(self):
        """
        This schema example is from documentation http://avro.apache.org/docs/1.7.6/gettingstartedpython.html
        """
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        deserializer = AvroJsonDeserializer(avro_schema)
        alyssa = {"name": "Alyssa", "favorite_number": 256}
        alyssa_full = {"name": "Alyssa", "favorite_number": 256, "favorite_color": None}
        alyssa_json = """{"name":"Alyssa","favorite_number":{"int":256},"favorite_color":null}"""
        self.assertEquals(serializer.to_json(alyssa), alyssa_json)
        self.assertEquals(deserializer.from_json(alyssa_json), alyssa_full)
        ben = {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
        ben_json = """{"name":"Ben","favorite_number":{"int":7},"favorite_color":{"string":"red"}}"""
        self.assertEquals(serializer.to_json(ben), ben_json)
        self.assertEquals(deserializer.from_json(ben_json), ben)
        lion = {"name": "Lion"}
        lion_full = {"name": "Lion", "favorite_number": None, "favorite_color": None}
        lion_json = """{"name":"Lion","favorite_number":null,"favorite_color":null}"""
        self.assertEquals(serializer.to_json(lion), lion_json)
        self.assertEquals(deserializer.from_json(lion_json), lion_full)

    def test_nested_union_records(self):
        schema_dict = {
            "namespace": "nested",
            "name": "OuterType",
            "type": "record",
            "fields": [{
                "name": "outer",
                "type": ["null", {
                    "name": "MiddleType",
                    "type": "record",
                    "fields": [{
                        "name": "middle",
                        "type": ["null", {
                            "name": "InnerType",
                            "type": "record",
                            "fields": [{
                                "name": "inner",
                                "type": "int"
                            }]
                        }]
                    }]
                }]
            }]
        }
        data1 = {"outer": {"middle": {"inner": 1}}}
        data2 = {"outer": {"middle": None}}
        avro1 = """{"outer":{"nested.MiddleType":{"middle":{"nested.InnerType":{"inner":1}}}}}"""
        avro2 = """{"outer":{"nested.MiddleType":{"middle":null}}}"""

        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        self.assertEquals(serializer.to_json(data1), avro1)
        self.assertEquals(serializer.to_json(data2), avro2)

        deserializer = AvroJsonDeserializer(avro_schema)
        self.assertEquals(deserializer.from_json(avro1), data1)
        self.assertEquals(deserializer.from_json(avro2), data2)

    def test_fixed_non_ascii(self):
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "WithFixed",
            "fields": [
                self.FIELD_FIXED
            ]
        }
        data = {"ffixed": b"(~^\xfbzoW\x13p\x19!4\x0b+\x00\x00"}
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        avro_json = serializer.to_json(data)
        self.assertEquals(avro_json, """{"ffixed":"(~^\\u00fbzoW\\u0013p\\u0019!4\\u000b+\\u0000\\u0000"}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_fixed_ascii(self):
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "WithFixed",
            "fields": [
                self.FIELD_FIXED
            ]
        }
        data = {"ffixed": b"fixed text here!"}
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        avro_json = serializer.to_json(data)
        self.assertEquals(avro_json, """{"ffixed":"fixed text here!"}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_bytes_field_non_ascii(self):
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "WithFixed",
            "fields": [
                {
                    "type": "bytes",
                    "name": "fbytes"
                }
            ]
        }
        data = {"fbytes": b"(~^\xfbzoW\x13p\x19!4\x0b+\x00\x00\x0b+\x00\x00"}
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        avro_json = serializer.to_json(data)
        self.assertEquals(avro_json, """{"fbytes":"(~^\\u00fbzoW\\u0013p\\u0019!4\\u000b+\\u0000\\u0000\\u000b+\\u0000\\u0000"}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)

    def test_bytes_field_ascii(self):
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "WithFixed",
            "fields": [
                {
                    "type": "bytes",
                    "name": "fbytes"
                }
            ]
        }
        data = {"fbytes": b"this is some long bytes field"}
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        serializer = AvroJsonSerializer(avro_schema)
        avro_json = serializer.to_json(data)
        self.assertEquals(avro_json, """{"fbytes":"this is some long bytes field"}""")
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, data)


class TestAvroJsonDeserializer(TestCase):
    def test_missing_nullable_field(self):
        schema_dict = {
            "type": "record",
            "name": "WithDefault",
            "fields": [
                {
                    "type": "string",
                    "name": "name"
                },
                {
                    "type": ["null", "int"],
                    "name": "version",
                    "default": None
                }
            ]
        }
        avro_json = """{"name":"mcnameface"}"""
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        deserializer = AvroJsonDeserializer(avro_schema)
        self.assertRaises(avro.errors.AvroTypeException, deserializer.from_json, avro_json)

    def test_unknown_fields_are_ignored(self):
        schema_dict = {
            "type": "record",
            "name": "BasicName",
            "fields": [
                {
                    "type": "string",
                    "name": "name"
                }
            ]
        }
        avro_json = """{"name":"todd","age":1}"""
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        json_data = AvroJsonDeserializer(avro_schema).from_json(avro_json)
        self.assertEquals(json_data, {"name": "todd"})

    def test_dict_with_unicode_bytes(self):
        schema_dict = {
            "namespace": "example.avro",
            "type": "record",
            "name": "WithBytes",
            "fields": [
                {
                    "type": "bytes",
                    "name": "fbytes"
                }
            ]
        }

        # byte arrays should be left alone
        byte_data = {"fbytes": b"(~^\xfbzoW\x13p\x19!4\x0b+\x00\x00\x0b+\x00\x00"}
        avro_schema = make_avsc_object(schema_dict, avro.schema.Names())
        self.assertEquals(AvroJsonDeserializer(avro_schema).from_dict(byte_data), byte_data)

        # unicode strings should be turned into iso-8859-1 bytes
        iso8859_data = {'fbytes': b"(~^\xfbzoW\x13p\x19!4\x0b+\x00\x00"}
        unicode_data = {u'fbytes': u'(~^\xfbzoW\x13p\x19!4\x0b+\x00\x00'}
        self.assertEquals(AvroJsonDeserializer(avro_schema).from_dict(unicode_data), iso8859_data)
