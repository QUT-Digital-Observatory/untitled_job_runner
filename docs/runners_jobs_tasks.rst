Runners and Jobs and Tasks (oh my)
==================================

Untitled Job Runner is structured around the three concepts of
:ref:`runners <runner_concept>`, :ref:`jobs <job_concept>`, and
:ref:`tasks <task_concept>`.

Jobs are multi-step workflows with a specific purpose. Each job is able to determine,
when requested by the runner, which tasks are the required next steps for the job. The
runner keeps track of jobs and calls on them to provide tasks, which the runner executes
as capacity allows.

.. image:: _images/UJR_runner_loop.*
    :alt: Diagram: The Runner sits at the centre, encompassing Jobs and Tasks. Within
        the Runner, the Runner.run loop contains job.get_runnable_tasks methods for
        each job, connecting each job to its tasks. Outside the Runner, configuration
        sources are taken in and state reporting is output.

Method references in diagram:

- :meth:`Runner.run`
- :meth:`Job.get_runnable_tasks`


.. index:: Job
.. _job_concept:

Job
---

Class: :class:`untitled_job_runner.Job`

A concrete and largely discrete piece of work to be done in code on the platform.
Jobs often have multiple steps (tasks) within them, and jobs can often be used for
multiple purposes and multiple projects (with appropriate configuration). The runner
will ask the job for any tasks, and the job assesses any relevant state and returns
tasks to the runner to be run.

There are no explicit dependencies between jobs in our model. Each job is just
responsible for itself and its tasks.

A job is responsible for determining what tasks should be run and when, but not
responsible for handling the running of those tasks or any immediate handling of
uncaught errors resulting from that run.


.. index:: Task
.. _task_concept:

Task
----

Class: :class:`untitled_job_runner.Task`

A smaller piece of code, a step within a job, often reusable. Jobs dynamically
determine tasks that make up the job, and decide when tasks are ready to be run, and
pass them to the runner to execute. The tasks are where the actual work is done.


.. index:: Runner
.. _runner_concept:

Runner
------

Class: :class:`untitled_job_runner.Task`

The runner is responsible for:

- Consolidating and interpreting configuration to determine what jobs should/should
  not be running
- Asking each job what task should be running right now - Running those tasks in a
  process pool
- Keeping track of which tasks are running using the task’s signature for deduplication
- Catching task failures and successes


.. _twitter_example:

Twitter data processing example
-------------------------------

This example is somewhat simplified, but derived from actual usage of Untitled Job
Runner by the `QUT Digital Observatory <https://www.qut.edu.au/ife/do>`_ to create
and manage their Twitter data collections.


Twitter collector job
~~~~~~~~~~~~~~~~~~~~~

A Twitter collector job is responsible for fetching and storing data from Twitter. This
may be used as a continuously running job, or
as a one-off job. Different instances of this job might collect different data, for
example results from different Twitter searches.

Tasks:

- Fetch data and store locally, with a given time period between calls (managed by the
  job)
- Resolve URLs in tweets
- Backup/archive local data, when the job decides it's time to back up

Tweet tidier job
~~~~~~~~~~~~~~~~

This job takes a collection of raw tweet data and transforms the data into a desired
tidy format (such as in a relational database). This may be a continuously running
job, picking up any new raw data and adding it to the tidy data archive, or it may be
a one-off job.

Tasks:

- Check for new raw data and fetch it if necessary
- Tidy new raw data and store it

Dataset summary job
~~~~~~~~~~~~~~~~~~~

This job can be run periodically, on a schedule, or manually as a one-off job to
perform desired queries on tidy data, such as status or summary queries or report
generation.

Tasks:

- Run specified query on dataset and store results
- Notify specified targets

Running jobs
~~~~~~~~~~~~

Jobs may be run by writing scripts specifying jobs and calling LocalJobsRunner on a
local machine, or on a server using a custom subclass of Runner which has its own
system of fetching and updating job configuration on the fly.
