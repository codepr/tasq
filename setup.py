from setuptools import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

# Dependencies
required = ['zmq', 'cloudpickle']

setup(
    name='tasq',
    version='0.1.0',
    description='A simple brokerless task queue implementation leveraging zmq and a naive '
                'implementation of the actor model to enqeue jobs on local or remote processes',
    long_description=readme,
    author='Andrea Giacomo Baldan',
    author_email='a.g.baldan@gmail.com',
    packages=['tasq', 'tasq.remote', 'tasq.cli'],
    install_requires=required,
    entry_points={
        'console_scripts': ['tasq = tasq.cli.main:main']
    }
)
