import src.media as media


class QueryJob:
    def __init__(self, search_params, only_not_embedded=False) -> None:
        self.search_params = search_params
        self.only_not_embedded = only_not_embedded

    def filter_objects(self, db, objs):
        if not self.only_not_embedded:
            return objs

        # Check which objects are already embedded.
        emb_objs = db.embeddings.find(
            {
                "video": {"$in": [obj.video_id() for obj in objs]},
            }
        )
        emb_obj_ids = set([emb_obj["video"] for emb_obj in emb_objs])

        # Filter out embedded objects.
        objs = [obj for obj in objs if obj.video_id() not in emb_obj_ids]

        return objs


class VideoJob(QueryJob):
    def __init__(
        self,
        search_params,
        only_not_embedded=False,
        frame_interval=50,
    ) -> None:
        super().__init__(search_params, only_not_embedded)
        self._frame_interval = frame_interval

    def get_objects(self, db):
        videos = db.videos.find(self.search_params)
        objs = [
            media.Video(
                source_path=video["path"],
                frame_interval=self._frame_interval,
                video_id=video["_id"],
            )
            for video in videos
        ]
        objs = self.filter_objects(db, objs)

        return objs


class ClipJob(QueryJob):
    def __init__(
        self,
        search_params,
        only_not_embedded=False,
        frame_interval=50,
    ) -> None:
        super().__init__(search_params, only_not_embedded)
        self._frame_interval = frame_interval

    def get_objects(self, db):
        clips = db.clips.aggregate(
            [
                {"$match": {**self.search_params}},
                {
                    "$lookup": {
                        "from": "videos",
                        "localField": "video",
                        "foreignField": "_id",
                        "as": "video",
                    }
                },
            ]
        )
        objs = [
            media.Clip(
                source_path=clip["video"][0]["path"],
                start_time=clip["start"],
                end_time=clip["end"],
                frame_interval=self._frame_interval,
                video_id=clip["video"][0]["_id"],
            )
            for clip in clips
        ]

        return objs


class CustomJob:
    pass
