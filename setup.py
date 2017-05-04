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
    version='0.1.0',
    description="Workload extraction, modelling and duplication for HPC systems",
    long_description=readme + '\n\n' + history,
    author="Tiago Quintino, Antonino Bonanni, Simon Smart",
    author_email='',
    packages=find_packages(),
    package_dir={
        'kronos': 'kronos'
    },
    scripts=[
        'bin/kronos-ingest',
        'bin/kronos-model',

        'bin/kronos-view-json',
        'bin/kronos-show-info',
        'bin/kronos-config',
        'bin/kronos-kpf',
        'bin/kronos-ksf',
        'bin/kronos-config-enquire',
        'bin/kronos-dummy-workload',

        'bin/kronos-generate-read-files',
        'bin/kronos-executor',
        'bin/kronos-raw-to-kpf',

        'bin/kronos-plot-kpf',
        'bin/kronos-map-2-kpf',
        'bin/kronos-dsh-2-kpf',

        'bin/kronos-run-perf',
        'bin/kronos-run-analysis',
    ],
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='kronos',
    test_suite='tests',
    tests_require=test_requirements
)
