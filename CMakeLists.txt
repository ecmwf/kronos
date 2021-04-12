cmake_minimum_required( VERSION 3.12 FATAL_ERROR )

find_package( ecbuild 3.4 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild )

project( kronos LANGUAGES C CXX )

# Dependencies and option

ecbuild_add_option(
    FEATURE MPI
    DESCRIPTION "Use system MPI libraries"
    REQUIRED_PACKAGES "NAME MPI COMPONENTS CXX C" )

# look for pmemio
ecbuild_add_option(
    FEATURE PMEMIO
    DESCRIPTION "Enable persistent memory support"
    REQUIRED_PACKAGES "NAME pmemio" )

if(pmemio_FOUND)
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
                RESULT_VARIABLE _enquire_read_file_multiplicity_status
                OUTPUT_VARIABLE KRONOS_READ_FILE_MULTIPLICITY )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config read_file_size_min
                RESULT_VARIABLE _enquire_read_size_min_status
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MIN )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config read_file_size_max
                RESULT_VARIABLE _enquire_read_size_max_status
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MAX )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/kronos_executor/kronos_executor/bin/kronos-enquire-global-config write_file_max_chunk
                RESULT_VARIABLE _enquire_write_file_max_chunk_status
                OUTPUT_VARIABLE KRONOS_WRITE_FILE_MAX_CHUNK )

if( NOT ( _enquire_read_file_multiplicity_status EQUAL 0
        AND _enquire_read_size_min_status EQUAL 0
        AND _enquire_read_size_max_status EQUAL 0
        AND _enquire_write_file_max_chunk_status EQUAL 0 ))
    ecbuild_critical("Failed to call kronos-enquire-global-config. Please make sure a Python interpreter is available.")
endif()

# And build the C components

ecbuild_declare_project()

### export package to other ecbuild packages
set( KRONOS_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/kronos_apps
    ${CMAKE_CURRENT_BINARY_DIR}/kronos_apps
)

set( KRONOS_LIBRARIES      "" )

get_directory_property( KRONOS_DEFINITIONS COMPILE_DEFINITIONS )

### add sources
add_subdirectory( kronos_apps )

### finalize project

ecbuild_add_resources( TARGET top_files SOURCES_DONT_PACK AUTHORS )
ecbuild_add_resources( TARGET top_files_1 SOURCES_DONT_PACK dev )
#ecbuild_add_resources( TARGET top_files SOURCES TODO AUTHORS README.md NOTICE LICENSE INSTALL ChangeLog COPYING )

ecbuild_install_project( NAME ${PROJECT_NAME} )

ecbuild_print_summary()

