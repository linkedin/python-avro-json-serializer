Python Avro JSON serializer
================

`AvroJsonSerializer` produces a JSON given avro schema and data.

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

`avro_json_serializer/test/test_avro_json_serializer.py` for more examples.