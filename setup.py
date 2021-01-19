import setuptools

description = "It runs things!"
with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    "Pebble>=4.5.3",
]

extras_require = {"development": ["nox", "pytest", "pre-commit"]}

setuptools.setup(
    name="untitled-job-runner",
    author="QUT Digital Observatory",
    author_email="digitalobservatory@qut.edu.au",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/QUT-Digital-Observatory/untitled_job_runner/",
    license="MIT",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.6",
    install_requires=install_requires,
    extras_require=extras_require,
)
