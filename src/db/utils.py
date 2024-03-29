import numpy as np


def validate_integrity(db, milv_coll, obj):
    # Check if object frames have already been inserted.
    db_frames = list(db["frames"].find({"video": obj.video_id()}))

    # Check if no frames found in database.
    if not len(db_frames) > 0:
        print("Found 0 database frames for object.")
        return False

    total_frames = len(obj)
    expected_num_frames = total_frames / obj._frame_interval

    # Check if inserted frame count differs significantly from expected cound.
    if total_frames > 0 and np.abs(expected_num_frames - len(db_frames)) > 25:
        print(f"Error: Found {len(db_frames)} frames, expected {expected_num_frames}")
        return False

    db_embs = list(
        db["embeddings"].find({"frame": {"$in": [r["_id"] for r in db_frames]}})
    )

    # Check if every frame has an embedding.
    if not len(db_embs) == len(db_frames):
        print(f"Error: Found {len(db_embs)} embeddings, expected {len(db_frames)}")
        return False

    milv_embs = milv_coll.query(
        expr=f"id in [{','.join([r['milvus_id'] for r in db_embs])}]"
    )

    # Check if all Milvus embeddings exist.
    if not len(milv_embs) == len(db_embs):
        print(f"Error: Found {len(milv_embs)} frames, expected {len(db_embs)}")
        return False

    return True
