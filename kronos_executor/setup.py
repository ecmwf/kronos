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

        'bin/kronos-collect-results',
        'bin/kronos-enquire-global-config',
        'bin/kronos-executor',

        'bin/kronos-format-config-model',
        'bin/kronos-format-config-exe',
        'bin/kronos-format-kprofile',
        'bin/kronos-format-kresults',
        'bin/kronos-format-kschedule',
        'bin/kronos-format-config-export',

        'bin/kronos-generate-read-files',

        'bin/kronos-summarise-kschedule',
        'bin/kronos-summarise-results',

        'bin/kronos-view-json'

    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos-executor',
    test_suite='tests',
    tests_require=test_requirements
)
