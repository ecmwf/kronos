
===============
Kronos executor
===============

Description
===========

The Kronos executor is a meta-scheduler: it takes the description of a workflow as a KSchedule file,
and submits the corresponding jobs to a HPC scheduler according to the dependencies described in the
KSchedule.

Running the executor
====================

Execution contexts
------------------

Kronos relies on execution contexts to run its jobs. See :py:mod:`kronos_executor.execution_context`
for a parameter reference and samples in the ``kronos_executor/kronos_executor/execution_contexts``
directory. Execution contexts are searched for by name, first in the current working directory, and
then in Kronos' ``execution_contexts`` directory. Therefore, defining a custom execution context is
done by writing a Python script similar to the existing ones, providing a ``Context`` class.

General configuration
---------------------

Additionally, a configuration file needs to be provided. A template is available in
``config/config.json.template``.  Once the configuration is ready, the executor can be run as
follows::

   kronos-executor -c <config-file> <kschedule>

Examining the output
====================

Once the executor has run, the output directory will contain one directory per job (in the same
order as inside the KSchedule). This directory contains the JSON input extracted from the KSchedule
(used by the synthetic application), the job script that has been generated and submitted to the
scheduler, the output and error streams of the job, as well as any files the job puts into its
working directory.

In case of synthetic application jobs, the statistics can be summarised using the following
command::

   kronos-summarise-results <output-dir>

Reference documentation
=======================

.. toctree::
   :maxdepth: 2

   config
   kschedule
   events
