import hydra

from src.job import ClipJob, VideoJob


def get_objects(db, jobs):
    """Get objects from database."""
    jobs = [hydra.utils.instantiate(job) for job in jobs]
    objs = [obj for job in jobs for obj in job.get_objects(db)]
    return objs
