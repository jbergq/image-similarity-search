"""
This script retrieves documents from existing MongoDB database representing media objects, computes embeddings and inserts into Milvus vector database for similarity search.
"""

import os
import random

import cv2
import dotenv
import hydra
import numpy as np
import pymongo

from src.db.milvus import get_milvus_collection
from src.db.mongodb import get_objects

dotenv.load_dotenv()


@hydra.main(config_path="config", config_name="config")
def main(cfg):
    # Instantiate model.
    model = hydra.utils.instantiate(cfg.model)

    # Connect to MongoDB
    user, password, host, port = (
        os.getenv("MONGODB_USER"),
        os.getenv("MONGODB_PASSWORD"),
        os.getenv("MONGODB_HOST"),
        os.getenv("MONGODB_PORT"),
    )

    # Connect to DB.
    client = pymongo.MongoClient(f"mongodb://{user}:{password}@{host}:{port}/")
    db = client[os.getenv("MONGODB_DATABASE")]

    # Connect to Milvus.
    milv_collname = model.name  # Use model name as collection name.
    milv_coll = get_milvus_collection(milv_collname, model.embedding_dim)

    # Get objects from database.
    objects = get_objects(db, cfg.jobs)
    random.shuffle(objects)

    if cfg.limit_samples:
        objects = objects[: cfg.limit_samples]

    # Load frames and compute embeddings.
    for i, obj in enumerate(objects):
        print(f"Processing object {obj.name}.")

        for frames in obj.get_iter(chunk_size=25):
            images = [frame["frame"] for frame in frames]
            images = [cv2.cvtColor(image, cv2.COLOR_BGR2RGB) for image in images]
            images = np.array(images)

            # Insert frame timestamp to database
            frame_objs = db["frames"].insert_many(
                [
                    {
                        "timestamp": frame["timestamp"],
                        "video": obj.video_id(),
                    }
                    for frame in frames
                ]
            )
            frame_ids = frame_objs.inserted_ids

            out = model.extract_embeddings({"image": images})
            embs = out["embeddings"]

            emb_objs = milv_coll.insert(
                [
                    {
                        "object_id": str(frame_id),
                        "model_name": model.name,
                        "embeddings": emb.tolist(),
                    }
                    for frame_id, emb in zip(frame_ids, embs)
                ]
            )
            emb_ids = emb_objs.primary_keys

            db["embeddings"].insert_many(
                [
                    {
                        "frame": frame_id,
                        "milvus_id": str(emb_id),
                        "video": obj.video_id(),
                        "model_name": model.name,
                    }
                    for emb_id, frame_id in zip(emb_ids, frame_ids)
                ]
            )

        milv_coll.flush()
        if i % 100 == 0:
            print(f"################## Processed {i} objects. ##################")


if __name__ == "__main__":
    main()
