import nox


@nox.session
def check_formatting(session):
    session.install('black')
    session.run('black', '--check', '.')
    # Uses black config from pyproject.toml


@nox.session
def lint(session):
    session.install('flake8')
    session.run('flake8')


@nox.session
def test(session):
    session.install('pytest')
    session.install('.')
    session.run('pytest')
