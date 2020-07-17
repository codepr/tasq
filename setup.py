import unittest
from setuptools import setup

with open("README.md") as readme_file:
    readme = readme_file.read()


def test_suite():
    test_loader = unittest.TestLoader()
    tests = test_loader.discover("tests", pattern="*_test.py")
    return tests


# Dependencies
required = ["zmq", "cloudpickle", "redis", "pika"]

setup(
    name="tasq",
    version="1.2.1",
    description="A simple task queue implementation leveraging zmq and a naive "
    "implementation of the actor model to enqeue jobs on local or remote processes",
    long_description=readme,
    author="Andrea Giacomo Baldan",
    author_email="a.g.baldan@gmail.com",
    packages=["tasq", "tasq.actors", "tasq.remote", "tasq.cli"],
    install_requires=required,
    scripts=["tq"],
    test_suite="setup.test_suite",
)
