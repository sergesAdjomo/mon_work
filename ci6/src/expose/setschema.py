from pyspark.sql.types import *
import json


def avro_to_spark_schema(avro_type):
    map_type = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "bytes": ByteType(),
    }

    if isinstance(avro_type, list):
        non_null_types = [i for i in avro_type if i != "null"]
        if len(non_null_types) == 1:
            return avro_to_spark_schema(non_null_types[0])
        else:
            raise Exception("Schem comlexity not covert yet !")
    elif isinstance(avro_type, dict):
        type_type = avro_type["type"]
        logical_type = avro_type.get("logicalType")
        if type_type == "long":
            if logical_type == "timestamp-millis":
                return TimestampType()
            elif logical_type == "date":
                return DateType()
            else:
                return LongType()
        elif type_type == "int":
            if logical_type == "date":
                return DateType()
            else:
                return IntegerType()
        if type_type == "enum":
            return StringType()

        if type_type == "record":
            fields = [
                StructField(field["name"], avro_to_spark_schema(field["type"]), True)
                for field in avro_type["fields"]
            ]
            return StructType(fields)
        elif type_type == "array":
            return ArrayType(avro_to_spark_schema(avro_type["items"]))
        elif type_type == "map":
            return MapType(StringType(), avro_to_spark_schema(avro_type["values"]))
        else:
            raise Exception(f"Unsupported type: {type_type}")
    else:
        return map_type.get(avro_type, StringType())


def build_spark_schema(avro_schema):
    avro_schema = json.loads(avro_schema)
    fields = avro_schema["fields"]
    struct_fields = [
        StructField(field["name"], avro_to_spark_schema(field["type"]), True)
        for field in fields
    ]
    return StructType(struct_fields)
