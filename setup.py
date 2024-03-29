#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

test_requirements = [ ]

setup(
    author="Audrey Roy Greenfeld",
    author_email='jillian@dabbleofdevops.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Utilities for interacting with AWS Batch",
    entry_points={
        'console_scripts': [
            'prefect_aws_batch=prefect_aws_batch.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='prefect_aws_batch',
    name='prefect_aws_batch',
    packages=find_packages(include=['prefect_aws_batch', 'prefect_aws_batch.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/dabble-of-devop-bioanalyze/prefect_aws_batch',
    version='0.1.0',
    zip_safe=False,
)
