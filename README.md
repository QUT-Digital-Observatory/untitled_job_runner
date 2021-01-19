# untitled job runner

Untitled Job Runner is a flexible and extensible Python framework for running jobs
and tasks. Jobs can be run locally or elsewhere, and we try to make few assumptions
about the nature of the jobs - you can run a mix of scheduled, continuous, or ad hoc
jobs which may or may not share tasks with other jobs.

This framework has evolved out of a university research infrastructure facility that
collects data, stores it, and processes it in a variety of ways for a variety of
research projects with differing needs. The initial development has been with
a data infrastructure / data science use case in mind, and we hope it will be useful
for other use cases as well.
