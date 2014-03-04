Python Avro JSON serializer
================

[AvroJsonSerializer](avro_json_serializer/__init__.py#L28) serializes data into a JSON format using AVRO schema.

Why do we need serializer instead of just dumping into JSON?
* validation that your data matches the schema
* serialization of unions (see [SimpleExample](#simple-example) below)
* some Avro JSON deserializers expect fields in JSON in the same order as in the schema

Binary distribution can be found on [pypi](https://pypi.python.org/pypi/avro_json_serializer/).

## Simple example:

```python

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
avro_schema = avro.schema.make_avsc_object(schema_dict, avro.schema.Names())
serializer = AvroJsonSerializer(avro_schema)

self.assertEquals(serializer.to_json({"name": "Alyssa", "favorite_number": 256}),
                  """{"name":"Alyssa","favorite_number":{"int":256},"favorite_color":null}""")

self.assertEquals(serializer.to_json({"name": "Ben", "favorite_number": 7, "favorite_color": "red"}),
                  """{"name":"Ben","favorite_number":{"int":7},"favorite_color":{"string":"red"}}""")

self.assertEquals(serializer.to_json({"name": "Lion"}),
                  """{"name":"Lion","favorite_number":null,"favorite_color":null}""")
```

## Another example:

```python

# need to serialize this data
data = {
    "ffloat": 1.0,
    "funion_null": None,
    "flong": 1L,
    "fdouble": 2.0,
    "ffixed": "1234567890123456",
    "fint": 1,
    "fstring": "hi there",
    "frec": {
        "subfint": 2
    }
}

# according to this schema:

schema_dict = {
    "fields": [{"name": "fint", "type": "int"},
            {"name": "flong", "type": "long"},
            {"name": "fstring", "type": "string"},
            {"name": "ffixed",
             "size": 16,
             "type": {"name": "fixed_16", "size": 16, "type": "fixed"}},
            {"name": "frec",
             "type": {"fields": [{"name": "subfint", "type": "int"}],
                      "name": "Rec",
                      "type": "record"}},
            {"name": "funion_null", "type": ["int", "null"]},
            {"name": "ffloat", "type": "float"},
            {"name": "fdouble", "type": "double"}],
    "name": "all_field",
    "namespace": "com.some.thing",
    "type": "record"
}

avro_schema = avro.schema.make_avsc_object(schema_dict, avro.schema.Names())

serializer = AvroJsonSerializer(avro_schema)
json_str = serializer.to_json(data)

print json_str
> {"fint":1,"flong":1,"fstring":"hi there","ffixed":"1234567890123456","frec":{"subfint":2},"funion_null":null,"ffloat":1.0,"fdouble":2.0}

```

See [tests](avro_json_serializer/test/test_avro_json_serializer.py) for more examples.


## How to run tests
```bash
python-avro-json-serializer$ virtualenv env
python-avro-json-serializer$ source venv/bin/activate
(venv)python-avro-json-serializer$ pip install nose
(venv)python-avro-json-serializer$ pip install -r requirements.txt
(venv)python-avro-json-serializer$ nosetests
.........
----------------------------------------------------------------------
Ran 9 tests in 0.052s

OK
```

## License

Python Avro JSON serializer is licensed under the terms of the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
