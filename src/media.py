from bson.objectid import ObjectId

from ext.video_utils.video_utils import load_recording_frames


class Media:
    def __init__(self, source_path) -> None:
        self.source_path = source_path

    def start(self):
        pass

    def end(self):
        pass

    def frame_interval(self):
        pass

    def get_iter(self, chunk_size=100):
        frames = []

        for frame in load_recording_frames(
            self.source_path,
            frame_interval=self.frame_interval(),
            start_time=self.start(),
            end_time=self.end(),
        ):
            frames.append(frame)

            if len(frames) == chunk_size:
                yield frames
                frames = []


class Frame(Media):
    pass


class Clip(Media):
    def __init__(
        self, source_path, start_time, end_time, frame_interval=50, video_id=None
    ) -> None:
        super().__init__(source_path)

        self.start_time = start_time
        self.end_time = end_time
        self._frame_interval = frame_interval
        self._video_id = video_id

    def start(self):
        return self.start_time

    def end(self):
        return self.end_time

    def frame_interval(self):
        return self._frame_interval

    def video_id(self):
        return ObjectId(self._video_id)

    @property
    def name(self):
        return self.source_path


class Video(Media):
    def __init__(self, source_path, frame_interval=50, video_id=None) -> None:
        super().__init__(source_path)

        self._frame_interval = frame_interval
        self._video_id = video_id

    def start(self):
        return 0

    def end(self):
        return None

    def frame_interval(self):
        return self._frame_interval

    def video_id(self):
        return ObjectId(self._video_id)

    @property
    def name(self):
        return self.source_path
