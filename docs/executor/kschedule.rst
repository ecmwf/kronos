
=====================
KSchedule file format
=====================

A KSchedule file contains a JSON object. The following properties may
be defined:

Common properties
=================

========  =======  ========  ====================================================
Property  Type     Required  Description
========  =======  ========  ====================================================
version   number   Yes       Version number of the KSchedule format (currently 3)
tag       string   Yes       Magic value to help test validity of KSchedule file
                             (currently ``KRONOS-KSCHEDULE-MAGIC``)
created   string   Yes       The creation timestamp (following RFC 3339)
uid       integer  No        The UID of the user who created this file
========  =======  ========  ====================================================

Prologue and epilogue
=====================

Custom tasks such as setting up and tearing down servers can be executed before and after running
the jobs. The ``prologue`` and ``epilogue`` properties are objects with a ``tasks`` array containing
a list of scripts to be run. For instance::

   {
     [...]
     "prologue": {
       "tasks": ["create-input", "start-server"]
     },
     "epilogue": {
       "tasks": ["stop-server", "wipe-output"]
     },
     [...]
   }

Jobs
====

The ``jobs`` property contains the actual list of jobs to be run. Each job can be either a call to
the synthetic application, or an external benchmarking job.

Common properties
-----------------

===========  =======  ========  ===========================================================
Property     Type     Required  Description
===========  =======  ========  ===========================================================
start_delay  number   No        Start delay relative to the first job (in seconds)
timed        boolean  No        If true, the job will be accounted for in the total runtime
metadata     object   Yes       Job metadata: ``job_name`` and ``workload_name`` can be set
depends      array    No        Job dependencies, see below
===========  =======  ========  ===========================================================

In case the executor runs in "scheduler" mode (meaning the dependencies are handled by the
scheduler), the ``depends`` array should contain the list of job ids the current job depends on.
Otherwise, if the executor runs in "events" mode, the ``depends`` array should contain objects
describing the events. See :doc:`events` for a description of the events.

Synthetic application jobs
--------------------------

A synthetic application job is identified by the presence of a ``frames`` property, as described in
the synthetic application input format description (:doc:`../synapp/input`). The following
additional properties can be set:

=========  =======  ========  ============================================
Property   Type     Required  Description
=========  =======  ========  ============================================
repeat     integer  No        Number of times this job should be repeated
num_procs  integer  Yes       Number of processes that the job will run on
=========  =======  ========  ============================================

External jobs
-------------

An external job is identified by the presence of a ``job_class`` property. This should point to a
Python module, either in ``kronos_executor/kronos_executor/job_classes``, or in the Kronos executor
current working directory. This job class should generate a job script running the external job
given the input parameters.

In addition to the parameters provided by the executor, the required ``config_params`` property may
contain additional user parameters to be sent along to the job class.

