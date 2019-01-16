#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('../README.rst') as readme_file:
    readme = readme_file.read()

with open('../HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='kronos-executor',
    version='0.6.0',
    description="Execution of a HPC workload on a target system",
    long_description=readme + '\n\n' + history,
    author="Tiago Quintino, Antonino Bonanni, Simon Smart",
    author_email='',
    packages=find_packages(),
    package_dir={
        'kronos_executor': 'kronos_executor'
    },
    scripts=[

        'kronos_executor/bin/kronos-collect-results',
        'kronos_executor/bin/kronos-enquire-global-config',
        'kronos_executor/bin/kronos-executor',

        'kronos_executor/bin/kronos-format-config-exe',
        'kronos_executor/bin/kronos-format-kprofile',
        'kronos_executor/bin/kronos-format-kresults',
        'kronos_executor/bin/kronos-format-kschedule',

        'kronos_executor/bin/kronos-generate-read-files',

        'kronos_executor/bin/kronos-summarise-kschedule',
        'kronos_executor/bin/kronos-summarise-results',

        'kronos_executor/bin/kronos-view-json'

    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos-executor',
    test_suite='tests',
    tests_require=test_requirements
)
