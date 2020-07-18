from setuptools import setup

with open("README.md") as readme_file:
    readme = readme_file.read()


# Dependencies
required = ["zmq", "cloudpickle", "redis", "pika"]

setup(
    name="tasq",
    version="1.2.2",
    description="A simple task queue implementation leveraging zmq and a naive "
    "implementation of the actor model to enqeue jobs on local or remote processes",
    long_description=readme,
    author="Andrea Giacomo Baldan",
    author_email="a.g.baldan@gmail.com",
    packages=["tasq", "tasq.actors", "tasq.remote", "tasq.cli", "tasq.worker"],
    install_requires=required,
    scripts=["tq"],
    test_suite="tests",
)
