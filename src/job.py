import src.media as media


class QueryJob:
    def __init__(self, search_params) -> None:
        self.search_params = search_params


class VideoJob(QueryJob):
    def __init__(self, search_params, frame_interval=50) -> None:
        super().__init__(search_params)
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

        return objs


class ClipJob(QueryJob):
    def __init__(self, search_params, frame_interval=50) -> None:
        super().__init__(search_params)
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
