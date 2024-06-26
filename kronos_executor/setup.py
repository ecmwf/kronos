#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

requirements = [
    "jinja2",
    "jsonschema",
    "numpy",
    "setuptools",
    "strict-rfc3339"
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='kronos-executor',
    version='0.8.2',
    description="Execution of a HPC workload on a target system",
    long_description="Execution of a HPC workload on a target system",
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

        'kronos_executor/bin/kronos-notify',
        'kronos_executor/bin/kronos-report-statistics',

        'kronos_executor/bin/kronos-summarise-kschedule',
        'kronos_executor/bin/kronos-summarise-results',

        'kronos_executor/bin/kronos-validate-results',
        'kronos_executor/bin/kronos-view-run',
        'kronos_executor/bin/kronos-view-json'

    ],
    package_data={
        'kronos_executor': [
            'config_format/exe_config_schema.json',
            'execution_contexts/trivial_run.sh',
            'io_formats/profile_schema.json',
            'io_formats/results_schema.json',
            'io_formats/schedule_schema.json',
            'job_classes/lsf_filter.sh',
            'job_templates/composite.sh',
            'job_templates/io_master.sh',
            'job_templates/synapp.sh',
            'kronos_events/schema.json'
        ]
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos-executor',
    test_suite='tests',
    tests_require=test_requirements
)
