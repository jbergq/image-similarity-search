from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections


def get_milvus_collection(collection_name, emb_dim):
    connections.connect("default", host="localhost", port="19530")  # TODO: load config
    fields = [
        FieldSchema(
            name="id",
            dtype=DataType.INT64,
            is_primary=True,
            auto_id=True,
        ),
        FieldSchema(
            name="object_id",
            dtype=DataType.VARCHAR,
            max_length=100,
        ),
        FieldSchema(
            name="model_name",
            dtype=DataType.VARCHAR,
            max_length=100,
        ),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=emb_dim),
    ]
    schema = CollectionSchema(fields)
    milv_coll = Collection(collection_name, schema, consistency_level="Strong")

    return milv_coll
