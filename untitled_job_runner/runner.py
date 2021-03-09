"""
Runner and Local Runner

Runner responsibilities: keep track of the state of what is to be running.
"""
from abc import abstractmethod
from collections import defaultdict
import datetime
import logging
import multiprocessing as mp
import time
from typing import List

import pebble

from untitled_job_runner.job import Job, JobDone


logger = logging.getLogger(__name__)


class Runner:
    def get_changed_jobs(self):
        """
        Stub for detecting change in the state of jobs.

        This allows us flexibility in configuration of where the state of jobs
        is stored.

        """
        return [], []

    def fetch_job(self, job_name):
        """Return a fully initialised job object, corresponding to the given name."""
        pass

    def update_jobs(self):
        """
        Update the jobs that are running.

        New jobs are created if needed, jobs that no longer exist are deleted and
        stopped. A job that has changed will be deleted and stopped, then started
        again with the new config.

        """
        delete_jobs, create_jobs = [], []

        try:
            delete_jobs, create_jobs = self.get_changed_jobs()

        except Exception as e:
            logger.exception(e)

        for job_name in delete_jobs:

            # Clean up any running tasks for deleted jobs
            for task_info, task in self.tasks_to_do[job_name]:
                task.cancel()

            del self.tasks_to_do[job_name]

        for job_name in create_jobs:

            try:
                job = self.fetch_job(job_name)

                self.jobs[job.job_name] = job

            except Exception as e:
                logger.exception(e)

    def report_exception(self, job_name, task_info, exception, message):
        pass

    def report_job_done(self, job_name):
        pass

    def report_task_done(self, job_name, task_info, result):
        pass

    @abstractmethod
    def check_still_running(self):
        """Interface for checking whether to keep running."""
        return True

    def run(self):

        self.tasks_to_do = defaultdict(dict)

        while self.check_still_running():
            # Check whether any tasks are finished - catching and logging exceptions?

            for job_name, tasks in self.tasks_to_do.items():
                to_remove = set()

                for task_info, task in tasks.items():
                    if task.done():

                        to_remove.add(task_info)

                        try:
                            # Make sure to observe the result in case it raised an
                            # error.
                            result = task.result()
                            # Task has finished
                            logger.debug(f"Completed task for {job_name}: {task_info}")

                            # Report task success here
                            self.report_task_done(job_name, task_info, result)

                        except Exception as e:
                            # Task has thrown an error
                            # TODO: add job_name and task details to error message
                            logger.exception(e)

                            self.report_exception(
                                job_name, task_info, e, "Unexpected error in task"
                            )

                for task_info in to_remove:
                    del self.tasks_to_do[job_name][task_info]

            remove_jobs = set()

            # Asks jobs for new tasks
            for job_name, job in self.jobs.items():

                if (
                    job.next_check is None
                    or datetime.datetime.utcnow() >= job.next_check
                ):

                    try:
                        new_tasks = job.get_runnable_tasks()

                    except JobDone:
                        logger.debug(f"Job {job_name} is all done!")
                        self.report_job_done(job_name)
                        remove_jobs.add(job_name)

                    for task in new_tasks:
                        if len(self.tasks_to_do[job_name]) >= self.max_tasks_per_job:
                            logger.debug(
                                f"Job {job_name} is at the maximum number "
                                f"of tasks allowed in the pool per job "
                                f"({self.max_tasks_per_job}) and so new "
                                f"tasks are being discarded."
                            )
                            break

                        task_callable, task_args, task_kwargs, signature = task
                        task_info = (task_callable, signature)

                        if task_info not in self.tasks_to_do[job_name]:
                            task_future = self.pool.schedule(
                                task_callable, task_args, task_kwargs
                            )
                            self.tasks_to_do[job_name][
                                (task_callable, signature)
                            ] = task_future

            for job_name in remove_jobs:
                del self.jobs[job_name]

            self.update_jobs()

            time.sleep(self.check_interval)


class LocalJobsRunner(Runner):
    def __init__(
        self,
        jobs: List[Job],
        check_interval=60,
        min_pool_processes=1,
        max_tasks_per_job=None,
    ):
        """
        :param check_interval: number of seconds to wait in between checking for new
        tasks
        :param max_tasks_per_job: Jobs are limited to having this number of tasks
        waiting in the pool at once, to reduce the possibility of a single job
        flooding the pool. Defaults to the size of the process pool.
        :param min_pool_processes: The minimum size of the process pool to execute
        tasks. Defaults to the minimum of the detected number of CPUs or this value.
        """

        self.jobs = {job.job_name: job for job in jobs}

        pool_size = max(min_pool_processes, mp.cpu_count())

        self.pool = pebble.ProcessPool(pool_size, max_tasks=1)
        self.max_tasks_per_job = max_tasks_per_job or pool_size
        self.check_interval = check_interval

    def check_still_running(self):
        """The local jobs runner keeps going until all of the jobs are complete"""
        return self.jobs
