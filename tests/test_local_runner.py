import untitled_job_runner as ujr
import os.path
import logging


def file_write_task(filename):
    with open(filename, "w") as file:
        file.write("hello")


class BasicTestJob(ujr.Job):
    def __init__(self, test_filename):
        self.test_filename = test_filename
        super().__init__("basic_test_job")

    def get_runnable_tasks(self):
        """Return a task that sleeps, then writes to a file"""
        # If the file exists, raise JobDone
        if os.path.exists(self.test_filename):
            raise ujr.JobDone

        # Otherwise, create the file
        # (fn, args, kwargs, signature)
        return [(file_write_task, (self.test_filename,), dict(), self.test_filename)]


# TODO: add timeout (probs with pytest_timeout plugin)
def test_basic_run_and_stop(tmpdir):
    """
    Just make sure the runner loop starts, executes a given task, and stops
    """
    test_filename = os.path.join(tmpdir, "test_file.txt")
    # Initialise job incl config
    job = BasicTestJob(test_filename)
    # Start runner with job
    runner = ujr.LocalJobsRunner([job], check_interval=1)
    runner.run()

    # Check that the file is written, and the runner terminates.
    assert os.path.exists(test_filename)


# TODO: parametrised test case for the time to next check:
# None, in the past, 10 seconds from now, invalid/not a datetime

# TODO: checking of number of tasks per job in the pool


class ExceptionTestJob(ujr.Job):
    def __init__(self, test_filename):
        self.test_filename = test_filename
        super().__init__("exception_test_job")

    def get_runnable_tasks(self):
        """Return a task that sleeps, then writes to a file"""
        # If the file exists, raise JobDone
        if os.path.exists(self.test_filename):
            raise ujr.JobDone

        # Otherwise, create the file
        return [(raises_one_error, (self.test_filename,), {}, self.test_filename)]


def raises_one_error(test_filename):

    run_before = os.path.exists(test_filename)

    if run_before:
        return
    else:
        with open(test_filename, "w"):
            pass

        raise TypeError("Task has not been run before.")


def test_catches_exception_and_continues(tmpdir, caplog):
    """
    Just make sure the runner loop starts, executes a given task, and stops
    """
    test_filename = os.path.join(tmpdir, "test_file.txt")
    # Initialise job incl config
    job = ExceptionTestJob(test_filename)
    # Start runner with job
    runner = ujr.LocalJobsRunner([job], check_interval=1)
    runner.run()

    # Check that the file is written, and the runner terminates.
    assert os.path.exists(test_filename)

    for record in caplog.records:
        if record.levelname == "ERROR" and record.exc_info[0] is TypeError:
            break
    else:
        assert False


class MultiTaskJob(ujr.Job):
    def __init__(self, test_filename_prefix, n_files):
        self.test_filename_prefix = test_filename_prefix
        self.n_files = n_files

        super().__init__("multi_test_job")

    def get_runnable_tasks(self):
        """Return a new file to be created each time it's called."""
        tasks = []

        for i in range(self.n_files):
            filename = f"{self.test_filename_prefix}.{i}"
            if os.path.exists(filename):
                continue
            else:
                tasks.append((file_write_task, (filename,), {}, filename))

        if tasks:
            return tasks
        else:
            raise ujr.JobDone


def test_multiple_tasks_emitted(tmpdir, caplog):
    """
    Just make sure the runner loop starts, executes a given task, and stops
    """
    caplog.set_level(logging.DEBUG)
    test_filename = os.path.join(tmpdir, "test_file.txt")

    n_files = 10
    # Initialise job incl config
    job = MultiTaskJob(test_filename, n_files)
    # Start runner with job
    runner = ujr.LocalJobsRunner([job], check_interval=1)
    runner.run()

    # should see n_files completed.
    completed = 0

    for record in caplog.get_records(when="call"):
        if record.levelname == "DEBUG" and record.message.startswith("Complete"):
            completed += 1

    assert completed == n_files
