Contributing to Untitled Job Runner
===================================

Thanks so much for thinking about contributing! All help is appreciated, help with
documentation is extra appreciated.

Please `submit a pull request on GitHub
<https://github.com/QUT-Digital-Observatory/untitled_job_runner/pulls>`_   with any
changes.


Pre-commit
----------

Untitled Job Runner uses `pre-commit <https://pre-commit.com/>`_ to automatically
run linting and formatting on commit.

When you clone Untitled Job Runner, before committing any changes, we recommend you
`install pre-commit <https://pre-commit.com/#install>`_ and then run ``pre-commit
install`` to install the plugins listed in the ``.pre-commit-config.yaml`` file in the
root of the repository.

Using pre-commit like this helps avoid failing the CI tests on GitHub when pushing
your commits just because of minor formatting issues.


Testing your changes
--------------------

Untitled Job Runner uses `Nox <https://nox.thea.codes>`_ to manage running tests, among
other things.

To run the full test suite, from the root Untitled Job Runner directory, run::

    nox -s test

This is the same way the tests are run in CI on GitHub.

If you are editing the documentation, you can also use Nox to build the docs locally
by running::

    nox -s build_docs

You can then open the built HTML pages in ``docs/_build`` to view your changes.
