"""
Job interface

How do jobs indicate they are "done"?

Jobs raise StopIteration when no more work is to be done?
Later: How does the runner keep track of jobs that finished, and also interact with
the controller.

"""


import datetime as dt
from abc import abstractmethod
from typing import List, Tuple, Sequence, Dict, Callable, Optional, Hashable
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
    ) -> List[Tuple[Callable, Sequence, Dict, Optional[Hashable]]]:
        """
        Returns a list of tuples representing the next tasks to be done for this job.

        Representation of a task in the return value is a tuple containing:

        1. The callable for the task

        2. A sequence containing the arguments for the callable.

        3. A dictionary or similar structure containing the keyword arguments for the
        callable.

        4. An optional signature for the args and kwargs. The signature is used to
        uniquely represent the argqs and keyword arguments for a specific task, as a job
        can generate many tasks with different arguments. If no signature is specified,
        all tasks for a job with the same callable will be treated as the same, so only
        one of them will run at a time, regardless of the args and kwargs to that task.

        If the job is completely finished and doesn't need to be re-done, it can
        throw JobDone to signal completion.

        """
        pass
