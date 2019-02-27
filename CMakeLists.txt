# The old (original) kronos stuff is hidden now in the sandbox

cmake_minimum_required( VERSION 2.8.11 FATAL_ERROR )

project( kronos C CXX )

set( CMAKE_MODULE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
    ${CMAKE_MODULE_PATH}
    "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake"
    )

include( ecbuild_system NO_POLICY_SCOPE )

ecbuild_requires_macro_version( 2.0 )

# Dependencies and option

ecbuild_add_option(
    FEATURE MPI
    DESCRIPTION "Use system MPI libraries"
    REQUIRED_PACKAGES "MPI COMPONENTS CXX" )

# look for pmemio
find_package(pmemio)
message("HAVE_PMEMIO: ${HAVE_PMEMIO}")

if(PMEMIO_FOUND)
    set( USE_C_STD "-std=c99")
else()
    set( USE_C_STD "-std=c89")
endif()

# Add C flags to ensure standards compliance

if( CMAKE_C_COMPILER_ID STREQUAL "GNU" OR CMAKE_C_COMPILER_ID STREQUAL "Clang" )
    ecbuild_add_c_flags("${USE_C_STD} -pedantic -D_XOPEN_SOURCE=600")
elseif( CMAKE_C_COMPILER_ID STREQUAL "Intel" )
    ecbuild_add_c_flags("${USE_C_STD} -strict-ansi -D_XOPEN_SOURCE=600 -no-gcc")
elseif( CMAKE_C_COMPILER_ID STREQUAL "Cray" )
    ecbuild_add_c_flags("-h nognu -h notolerant -h conform -h stdc -h noc99 -D_XOPEN_SOURCE=600" )
endif()



if( ALLINEA_LIBS_DIR )
    message( STATUS "Linking with Allinea libraries for profiling" )
    # ecbuild_add_c_flags("-g")
    set( ECBUILD_C_LINK_FLAGS "-dynamic -L${ALLINEA_LIBS_DIR} -lmap-sampler-pmpi -lmap-sampler -Wl,--eh-frame-hd ${ECBUILD_C_LINK_FLAGS}" )
    set( CMAKE_C_LINK_FLAGS "-dynamic -L${ALLINEA_LIBS_DIR} -lmap-sampler-pmpi -lmap-sampler -Wl,--eh-frame-hd ${CMAKE_C_LINK_FLAGS}" )
else()
    set( ECBUILD_C_LINK_FLAGS "-lrt" )
    set( CMAKE_C_LINK_FLAGS "-lrt" )
endif()

# Configuration of the kronos project
# This is uniquely contained as a python module (so that the python doesn't need to be configured to be used/tested).

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config read_file_multiplicity
                OUTPUT_VARIABLE KRONOS_READ_FILE_MULTIPLICITY )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config read_file_size_min
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MIN )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config read_file_size_max
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MAX )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config write_file_max_chunk
                OUTPUT_VARIABLE KRONOS_WRITE_FILE_MAX_CHUNK )

# And build the C components

ecbuild_declare_project()

### export package to other ecbuild packages
set( KRONOS_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/kronos_apps
    ${CMAKE_CURRENT_BINARY_DIR}/kronos_apps
)

set( KRONOS_LIBRARIES      "" )

get_directory_property( KRONOS_DEFINITIONS COMPILE_DEFINITIONS )

### include directories

if (pmemio_FOUND)
    include_directories(
        ${KRONOS_INCLUDE_DIRS}
        ${PMEMIO_INCLUDE_DIRS}
        )
else()
    include_directories( ${KRONOS_INCLUDE_DIRS} )
endif()

add_subdirectory( kronos_apps )

### finalize project

ecbuild_add_resources( TARGET top_files SOURCES_DONT_PACK AUTHORS )
ecbuild_add_resources( TARGET top_files_1 SOURCES_DONT_PACK dev )
#ecbuild_add_resources( TARGET top_files SOURCES TODO AUTHORS README.md NOTICE LICENSE INSTALL ChangeLog COPYING )

ecbuild_pkgconfig(NAME kronos_synapps
                  DESCRIPTION "Execute synthetic workloads on HPC systems using synthetic applications" )

ecbuild_install_project( NAME ${PROJECT_NAME} )

ecbuild_print_summary()

