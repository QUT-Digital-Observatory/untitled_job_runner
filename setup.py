import setuptools

description = "Untitled job runner"
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="untitled-job-runner",
    author="QUT Digital Observatory",
    author_email="digitalobservatory@qut.edu.au",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/digital-observatory/untitled-job-runner",
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)