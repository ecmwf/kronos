#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

requirements = [
    "kronos-executor==0.7.0"
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='kronos-modeller',
    version='0.7.0',
    description="Modelling of an HPC workload to be executed by the kronos-executor",
    long_description="Modelling of an HPC workload to be executed by the kronos-executor",
    author="Tiago Quintino, Antonino Bonanni, Simon Smart",
    author_email='',
    packages=find_packages(),
    package_dir={
        'kronos-modeller': 'kronos-modeller'
    },
    scripts=[

        'kronos_modeller/bin/kronos-analyse-results',

        'kronos_modeller/bin/kronos-convert-dataset-to-kprofile',
        'kronos_modeller/bin/kronos-convert-kprofile-to-kresults',
        'kronos_modeller/bin/kronos-convert-kresults-to-kprofile',

        'kronos_modeller/bin/kronos-format-config-export',
        'kronos_modeller/bin/kronos-format-config-model',

        'kronos_modeller/bin/kronos-ingest-logs',
        'kronos_modeller/bin/kronos-inspect-dataset',
        'kronos_modeller/bin/kronos-model',
        'kronos_modeller/bin/kronos-plot-kprofile'

    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos-modeller',
    test_suite='tests',
    tests_require=test_requirements
)
