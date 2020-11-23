import setuptools

description = "Untitled Job Runner"
with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    "cryptography>=2.3.0",
    "Pebble>=4.5.3",
]

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
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires='>=3.6',
    install_requires=install_requires,
)
