"""
Job interface

How do jobs indicate they are "done"?

Jobs raise StopIteration when no more work is to be done?
Later: How does the runner keep track of jobs that finished, and also interact with
the controller.

"""


import datetime as dt
from abc import abstractmethod
from typing import Tuple, List, Callable, Optional, Hashable
import logging


logger = logging.getLogger(__name__)


class StopJob(StopIteration):
    pass


# Should this inherit from something more specific?
class JobDone(Exception):
    pass


# Job may need to give specific tasks some indication of high/low priority for
# the runner
class Job:
    def __init__(self, job_name: Hashable):
        self.job_name = job_name

    @property
    def next_check(self) -> Optional[dt.datetime]:
        """
        The datetime specifying when the job can next be checked for runnable tasks.

        If not None, :met:`Job.get_runnable_tasks` will not be called until on or after
        this datetime. This can be used, for example, to prevent expensive calls to
        external services happening on every cycle.

        """
        return None

    @abstractmethod
    def get_runnable_tasks(
        self,
    ) -> List[Tuple[Callable, Tuple]]:
        """
        Returns a list of tuples representing the next tasks to be done for this job.

        Representation of a task in the return value is a tuple containing:
        1. The function for the task
        2. A flat tuple containing the arguments for the task function, ready to
           be unpacked.

        Also returns either None, or a UTC datetime indicating when this function can be
        called after.

        If the job is completely finished and doesn't need to be re-done, it can
        throw JobDone to signal completion.

        Question for future: should we include a signature of the task or other method
        for more nuanced duplicate-checking?
        """
        pass
