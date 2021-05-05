# (C) Copyright 1996-2015 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

# - Try to find pmem.io
# Once done this will define
#  PMEMIO_FOUND - System has pmem.io
#  PMEMIO_INCLUDE_DIRS - The pmem.io include directories
#  PMEMIO_LIBRARIES - The libraries needed to use pmem.io
#  PMEMIO_DEFINITIONS - Compiler switches required for using pmem.io
#  PMEMIO_OBJ_LIBRARIES - Location of libpmemobj

if( NOT pmemio_FOUND )

    if( NOT DEFINED PMEMIO_PATH AND NOT "$ENV{PMEMIO_PATH}" STREQUAL "" )
        list( APPEND PMEMIO_PATH "$ENV{PMEMIO_PATH}" )
    endif()

    if( DEFINED PMEMIO_PATH )
        find_path(
            PMEMIO_INCLUDE_DIR
            NAMES libpmem.h libpmemobj.h
            PATHS ${PMEMIO_PATH}
            PATH_SUFFIXES include src/include
        )
        find_library(
            PMEMIO_BASE_LIBRARY
            NAMES pmem
            PATHS ${PMEMIO_PATH}
            PATH_SUFFIXES debug nondebug src/debug src/nondebug lib lib64
        )
        find_library(
            PMEMIO_OBJ_LIBRARY
            NAMES pmemobj
            PATHS ${PMEMIO_PATH}
            PATH_SUFFIXES debug nondebug src/debug src/nondebug lib lib64
        )

    endif()

    # And searchers for when PMEMIO_PATH is not defined, or nothing is found in those locations.

    find_path(
        PMEMIO_INCLUDE_DIR
        NAMES libpmem.h libpmemobj.h
        PATHS
        PATH_SUFFIXES include src/include
    )
    find_library(
        PMEMIO_BASE_LIBRARY
        NAMES pmem
        PATHS
        PATH_SUFFIXES debug nondebug src/debug src/nondebug lib lib64
    )
    find_library(
        PMEMIO_OBJ_LIBRARY
        NAMES pmemobj
        PATHS
        PATH_SUFFIXES debug nondebug src/debug src/nondebug lib lib64
    )

    # This allows pmemio to be made up of more than just libpmemobj.
    set( PMEMIO_LIBRARIES "${PMEMIO_OBJ_LIBRARY} ${PMEMIO_BASE_LIBRARY}" )
    set( PMEMIO_INCLUDE_DIRS "${PMEMIO_INCLUDE_DIR}" )

    # We always need the base library
    list( APPEND PMEMIO_OBJ_LIBRARY "${PMEMIO_BASE_LIBRARY}")

    include(FindPackageHandleStandardArgs)

    # Handle the QUIETLY and REQUIRED arguments and set pmemio_FOUND to TRUE
    find_package_handle_standard_args( pmemio DEFAULT_MSG PMEMIO_INCLUDE_DIR PMEMIO_OBJ_LIBRARY )

    mark_as_advanced( PMEMIO_INCLUDE_DIR PMEMIO_OBJ_LIBRARY )

endif()
