"""
Local Runner

Runner responsibilities: keep track of the state of what is to be running.
Does the deployed runner report doneness back to the controller? Especially for not
worrying about monitoring of one off jobs.

"""
from abc import abstractmethod
from collections import defaultdict
import datetime
import json
import logging
import multiprocessing as mp
import signal
import time
from typing import List

from cryptography.fernet import Fernet
import pebble
import requests

from untitled_job_runner.job import Job, JobDone


# TODO: fix this.
# Logging setup - NOT SUITABLE FOR PRODUCTION (need to use logging config file as in
# original platform code), this will output to a temporary log file which will be
# overwritten each time the file is run. Assumes this file is the entrypoint.
logging.basicConfig(filename="testing.log", filemode="w", level=logging.DEBUG)
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

                self.jobs[job.job_name] = {"job": job, "next_check": None}

            except Exception as e:
                logger.exception(e)

    def report_exception(self, job_name, task_info, exception, message):
        pass

    def report_job_done(self, job_name):
        pass

    def report_task_done(self, job_name, task_info):
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
                    try:
                        task.result(timeout=0)
                        # Task has finished
                        to_remove.add(task_info)
                        logger.debug(f"Completed task for {job_name}: {task_info}")

                        # Report task success here back to the controller later?
                        self.report_task_done(job_name, task_info)

                    except TimeoutError:
                        # Task hasn't finished yet
                        continue
                    except Exception as e:
                        # Task has thrown an error
                        to_remove.add(task_info)
                        # TODO: add job_name and task details to error message
                        logger.exception(e)

                        self.report_exception(
                            job_name, task_info, e, "Unexpected error in task"
                        )

                for task_info in to_remove:
                    del self.tasks_to_do[job_name][task_info]

            remove_jobs = set()

            # Asks jobs for new tasks
            for job_name, job_state in self.jobs.items():

                if (
                    job_state["next_check"] is None
                    or datetime.datetime.utcnow() <= job_state["next_check"]
                ):

                    try:
                        new_tasks, next_check = job_state["job"].get_runnable_tasks()

                    except JobDone:
                        logger.debug(f"Job {job_name} is all done!")
                        self.report_job_done(job_name)
                        remove_jobs.add(job_name)

                    self.jobs[job_name]["next_check"] = next_check

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

        self.jobs = {job.job_name: {"job": job, "next_check": None} for job in jobs}

        pool_size = max(min_pool_processes, mp.cpu_count())

        self.pool = pebble.ProcessPool(pool_size, max_tasks=1)
        self.max_tasks_per_job = max_tasks_per_job or pool_size
        self.check_interval = check_interval

    def check_still_running(self):
        """The local jobs runner keeps going until all of the jobs are complete"""
        return self.jobs


def PlatformJobsRunner(Runner):
    def __init__(
        self,
        config_path,
        check_interval=60,
        min_pool_processes=8,
        max_tasks_per_job=None,
    ):
        """
        :param check_interval: number of seconds to wait in between checking for new
        tasks
        :param max_tasks_per_job: Jobs are limited to having this number of tasks
        waiting in the pool at once, to reduce the possibility of a single job
        flooding the pool. Defaults to num_pool_processes.
        :param min_pool_processes: The minimum size of the process pool to execute 
        tasks. Defaults to the minimum of the detected number of CPUs or this value.

        """

        logger.debug(f"Initiasing PlatformJobsRunner with config {config_path}")

        with open(config_path, "r") as config_file:
            runner_config = json.load(config_file)

        self.config = runner_config

        self.data_dir = runner_config["data_dir"]
        self.controller_url = runner_config["controller_url"]
        self.auth = ("node", runner_config["node_secret"])
        self.decryptor = Fernet(runner_config["secret_enc_key"])

        # Check if this node is registered by trying to load the url from the file.
        # This is a holdover from the original job/node design, and works like this
        # to avoid breaking the existing job runner which will be running in parallel.
        try:
            with open(os.path.join(self.data_dir, "node_path.txt"), "r") as f:
                node_path = f.read()

        except Exception:
            logger.info("Registering this node with the controller")

            # Register this node
            register_body = {"hostname": socket.getfqdn()}
            r = requests.post(
                self.controller_url + "/api/node", data=register_body, auth=self.auth
            )
            r.raise_for_status()
            response_data = r.json()

            # We store the path of the node url only, and later combine it on load with
            # the URL specified in the actual config. This ensures we can migrate the
            # controller to a new URL easily.
            node_path = response_data["node_url"]

            with open(os.path.join(self.data_dir, "node_path.txt"), "w") as f:
                f.write(node_path)

        # The node_id is used for sending status and logging events.
        self.node_id = int(node_path.split("/")[-1])

        self.jobs_url = self.controller_url + f"/api/new_style_jobs/"
        # Exact URL to retrieve the jobs list for this node.
        self.node_jobs_url = self.jobs_url + "?node_id={self.node_id}"

        # Signal handlers to stop the run method gracefully.
        signal.signal(signal.SIGINT, self.mark_exit)
        signal.signal(signal.SIGTERM, self.mark_exit)

        pool_size = max(min_pool_processes, mp.cpu_count())
        self.pool = pebble.ProcessPool(pool_size, max_tasks=1)
        self.max_tasks_per_job = max_tasks_per_job or pool_size
        self.check_interval = check_interval

        self.jobs = {}
        self.stop = False

    def get_changed_jobs(self):
        """
        Compare current state of running jobs to desired state and return jobs to
        change.

        """
        r = requests.get(self.node_jobs_url, auth=self.auth, timeout=30)
        r.raise_for_status()

        controller_jobs = r.json()

        delete_jobs = []
        create_jobs = []

        # Detect new and update jobs in the controller spec
        for job_name, job_summary in controller_jobs.items():

            if job_name in self.jobs:
                # The job has changed, so delete and recreate it
                if (
                    job_summary["version_id"]
                    != self.jobs[job_name]["job"]["version_id"]
                ):
                    delete_jobs.append(job_name)
                    create_jobs.append(job_name)
            else:
                create_jobs.append(job_name)

        # Detect jobs that are no longer present on the controller
        for job_name in self.jobs:

            if job_name not in controller_jobs:
                delete_jobs.append(job_name)

        return delete_jobs, create_jobs

    def fetch_job(self, job_name):
        """Return a fully initialised job object, corresponding to the given name."""

        r = requests.get(self.jobs_url + job_name, auth=self.auth, timeout=30)
        r.raise_for_status()

        # TODO: validation of parameters
        config = r.json()

        parameters = config["parameters"]

        if config["secrets"] is not None:
            secrets = json.loads(
                self.decryptor.decrypt(config["job_secrets"].encode("ascii"))
            )
        else:
            secrets = {}

        # Construct the job config from each of the components. The precedence order is:
        # secrets overrides job parameters overrides node config
        job_config = {}
        job_config.update(self.config)
        job_config.update(parameters)
        job_config.update(secrets)

        # TODO: Need a mapping from job_types to classes.
        job = job_type_mapping[job_config["job_type"]](**job_config)

        return job

    def check_still_running(self):
        """The platform jobs runner keeps going until a signal is recieved."""
        return not self.stop

    def mark_exit(self, handler, frame):
        """Exit the loop on sigterm/sigint"""
        logger.info("Setting the runner to stop.")
        self.stop = False
