
=================
Input file format
=================

The input file for the sythetic application is a JSON object containing
configuration and the list of frames describing the workload.


Frames
======

The ``frames`` property of the JSON object is an array of frames, which are
arrays of kernels. A kernel is an object defined by its ``name`` property, and
kernel-dependent options, as listed in :doc:`kernels`.


Configuration properties
========================

I/O configuration
-----------------

======================  =======  ===============  ==================================================
Property                Type     Default          Description
======================  =======  ===============  ==================================================
file_read_path          string   ``read_cache``   Source directory for read kernels
file_write_path         string   ``write_cache``  Destination directory for write kernels
file_shared_path        string   ``shared``       Shared directory for chained read/write operations
file_read_multiplicity  integer  100              Number of different files in the read cache
file_read_size_min_pow  integer  12               Minimum read size (power of 2, bytes)
file_read_size_max_pow  integer  27               Maximum read size (power of 2, bytes)
======================  =======  ===============  ==================================================

Statistics output
-----------------

=====================  =======  =======================  ===========================================
Property               Type     Default                  Description
=====================  =======  =======================  ===========================================
statistics_path        string   ``statistics.kresults``  Path to the statistics file
print_statistics       boolean  False                    Print out statistics on the standard output
write_statistics_file  boolean  True                     Write the statistics file
=====================  =======  =======================  ===========================================

Notification setup
------------------

======================  =======  =======  ===========================================
Property                Type     Default  Description
======================  =======  =======  ===========================================
notification_host       string   Empty    Send notifications to this host if provided
notification_port       integer  7363     Send notifications to this port
job_num                 integer  0        Job number to put in the notifications
======================  =======  =======  ===========================================

Verbosity options
-----------------

=================  =======  =======  ================================
Property           Type     Default  Description
=================  =======  =======  ================================
enable_trace       boolean  False    Print detailed trace information
mpi_ranks_verbose  boolean  False    Print output from all MPI ranks
=================  =======  =======  ================================

Other options
-------------

================  =======  ==========  ==============================
Property          Type     Default     Description
================  =======  ==========  ==============================
nvdimm_root_path  string   Empty       Path to the persistent memory
num_procs         integer  (computed)  Number of processes
================  =======  ==========  ==============================

If ``num_procs`` is set, it must match the number of MPI processes the
synthetic application is run with.

