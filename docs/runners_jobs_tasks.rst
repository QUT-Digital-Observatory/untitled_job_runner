Runners and Jobs and Tasks (oh my)
==================================

Untitled Job Runner is structured around the three concepts of *runners*, *jobs*, and
*tasks*.

.. image:: _images/UJR_runner_loop.*

Job
---
A concrete and largely discrete piece of work to be done in code on the platform.
Jobs often have multiple steps (tasks) within them, and jobs can often be used for
multiple purposes and multiple projects (with appropriate configuration). There are
no explicit dependencies between jobs in our model. Each job is just responsible for
itself and its tasks. A job is responsible for determining what tasks should be run
and when, but not responsible for handling the running of those tasks or any
immediate handling of uncaught errors resulting from that run.

Task
----

A smaller piece of code, a step within a job, often reusable. Jobs dynamically
determine tasks that make up the job, and decide when tasks are ready to be run, and
pass them to the runner to execute. Jobs check any relevant database etc state in
order to determine what tasks to do, but tasks are where the actual work is done.

Runner
------

The runner is responsible for:

- Consolidating and interpreting configuration to determine what jobs should/should
  not be running
- Initialising/tearing down jobs (i.e., taking job parameters, node info, and
  secrets, and merging them via fetch_job_details)
- Asking each job what task should be running right now - Running those tasks in a
  process pool
- Keeping track of which tasks are running using the task’s signature for deduplication
- Catching task failures and successes
