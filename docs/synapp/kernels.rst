
=======
Kernels
=======

Memory access
=============

``memory``: memory access
-------------------------

Allocate and write to a memory region with the given size.

=========  ======  ========  ========================
Parameter  Type    Required  Description
=========  ======  ========  ========================
kb_mem     number  Yes       Amount of memory, in KiB
=========  ======  ========  ========================

``memory_persist``: persistent memory access
--------------------------------------------

Write the given amout of data to persistent memory

=========  ======  ========  ========================
Parameter  Type    Required  Description
=========  ======  ========  ========================
kb_mem     number  Yes       Amount of memory, in KiB
=========  ======  ========  ========================

Computation
===========

``cpu``: floating-point operations
----------------------------------

Perform a given number of floating-point operations

=========  ======  ========  ===================================
Parameter  Type    Required  Description
=========  ======  ========  ===================================
flops      number  Yes       Number of floating point operations
=========  ======  ========  ===================================

Network communication
=====================

``mpi``: MPI communications
---------------------------

Perform pairwise and/or collective MPI data transfer of a given amount of data

=============  =======  ========  ===============================================
Parameter      Type     Required  Description
=============  =======  ========  ===============================================
kb_collective  number   Yes       Data size for collective communications, in KiB
kb_pairwise    number   Yes       Data size for pairwise communications, in KiB
n_collective   integer  Yes       Number of collective communications
n_pairwise     integer  Yes       Number of pairwise communications
=============  =======  ========  ===============================================

File I/O
========

``file-read``: file input
-------------------------

Read a given amount of data from one or more files

==========  =======  ========  ==============================
Parameter   Type     Required  Description
==========  =======  ========  ==============================
kb_read     number   Yes       Amount of data to read, in KiB
mmap        boolean  Yes       Use memory-mapped files
invalidate  boolean  No        Invalidate the cache entry
n_read      integer  Yes       Number of read operations
files       array    No        Specific files to read from
==========  =======  ========  ==============================

If ``files`` is specified, the files will be read from the shared directory,
which means they must have been created beforehand by an external job or by a
``file-write`` kernel.

``file-write``: file output
---------------------------

Write a given amount of data to one or more files

==============  =======  ========  =================================
Parameter       Type     Required  Description
==============  =======  ========  =================================
kb_write        number   Yes       Amount of data to write, in KiB
n_files         integer  Yes       Number of files to write to
files           array    No        Specific files to write to
n_write         integer  Yes       Number of write operations
continue_files  boolean  No        Do not close the file descriptors
==============  =======  ========  =================================

If ``files`` is specified, ``n_files`` must be equal to the number of files in
the list. The files are written to the shared directory and can be accessed by
future kernels or jobs.

``fs_metadata``: file system metadata handling
----------------------------------------------

Create and remove a given number of directories on the file system

=========  =======  ========  ==========================================
Parameter  Type     Required  Description
=========  =======  ========  ==========================================
n_mkdir    integer  Yes       Number of directories to create and remove
=========  =======  ========  ==========================================

