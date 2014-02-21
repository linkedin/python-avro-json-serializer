Python Avro JSON serializer
================

`AvroJsonSerializer` produces a JSON given avro schema and data.

## Example:

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



```