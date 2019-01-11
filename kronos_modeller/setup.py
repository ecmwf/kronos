#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('../README.rst') as readme_file:
    readme = readme_file.read()

with open('../HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "kronos-executor==0.6.0"
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='kronos-modeller',
    version='0.6.0',
    description="Modelling of an HPC workload to be executed by the kronos-executor",
    long_description=readme + '\n\n' + history,
    author="Tiago Quintino, Antonino Bonanni, Simon Smart",
    author_email='',
    packages=find_packages(),
    package_dir={
        'kronos-modeller': 'kronos-modeller'
    },
    scripts=[

        'bin/kronos-analyse-results',

        'bin/kronos-convert-dataset-to-kprofile',
        'bin/kronos-convert-kresults-to-kprofile',
        'bin/kronos-convert-kprofile-to-kresults',

        'bin/kronos-ingest-logs',
        'bin/kronos-inspect-dataset',

        'bin/kronos-model',
        'bin/kronos-plot-kprofile',

        'bin/kronos-validate-results',

        # 'bin/kronos-generate-test-workload',

    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos-modeller',
    test_suite='tests',
    tests_require=test_requirements
)
