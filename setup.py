#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='kronos',
    version='0.4.0',
    description="Workload extraction, modelling and duplication for HPC systems",
    long_description=readme + '\n\n' + history,
    author="Tiago Quintino, Antonino Bonanni, Simon Smart",
    author_email='',
    packages=find_packages(),
    package_dir={
        'kronos': 'kronos'
    },
    scripts=[
        'bin/kronos-ingest-logs',
        'bin/kronos-model',

        'bin/kronos-view-json',
        'bin/kronos-format-config-model',
        'bin/kronos-format-config-exe',
        'bin/kronos-format-kprofile',
        'bin/kronos-format-kresults',
        'bin/kronos-format-kschedule',
        'bin/kronos-format-config-export',

        'bin/kronos-generate-read-files',
        'bin/kronos-executor',
        'bin/kronos-convert-dataset-to-kprofile',
        'bin/kronos-inspect-dataset',

        'bin/kronos-plot-kprofile',
        'bin/kronos-convert-kresults-to-kprofile',
        'bin/kronos-convert-kprofile-to-kresults',

        'bin/kronos-summarise-kschedule',
        'bin/kronos-summarise-results',
        'bin/kronos-collect-results',
        'bin/kronos-validate-results',
        'bin/kronos-analyse-results',

        # development tools
        'bin/kronos-enquire-global-config',
        'bin/kronos-generate-dummy-jobs',
        
        # additional tools
        'bin/kronos-find-and-match-external-apps',
        'bin/kronos-write-external-app-runs',
        'bin/kronos-generate-kschedule',
        'bin/kronos-raps-output-to-kpf'

    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos',
    test_suite='tests',
    tests_require=test_requirements
)
