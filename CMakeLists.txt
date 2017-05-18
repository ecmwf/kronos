# The old (original) kronos stuff is hidden now in the sandbox

cmake_minimum_required( VERSION 2.8.11 FATAL_ERROR )

project( kronos C CXX )

set( CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild/cmake")

include( ecbuild_system NO_POLICY_SCOPE )

ecbuild_requires_macro_version( 2.0 )

# Dependencies and option

ecbuild_add_option(
    FEATURE MPI
    DESCRIPTION "Use system MPI libraries"
    REQUIRED_PACKAGES "MPI COMPONENTS CXX" )

# Add C flags to ensure standards compliance

if( CMAKE_C_COMPILER_ID STREQUAL "GNU" OR CMAKE_C_COMPILER_ID STREQUAL "Clang" )
    ecbuild_add_c_flags("-std=c89 -pedantic -D_XOPEN_SOURCE=600")
elseif( CMAKE_C_COMPILER_ID STREQUAL "Intel" )
    ecbuild_add_c_flags("-std=c89 -strict-ansi -D_XOPEN_SOURCE=600 -no-gcc")
elseif( CMAKE_C_COMPILER_ID STREQUAL "Cray" )
    ecbuild_add_c_flags("-h nognu -h notolerant -h conform -h stdc -h noc99 -D_XOPEN_SOURCE=600" )
endif()

if( ALLINEA_LIBS_DIR )
    message( STATUS "Linking with Allinea libraries for profiling" )
    # ecbuild_add_c_flags("-g")
    set( ECBUILD_C_LINK_FLAGS "-dynamic -L${ALLINEA_LIBS_DIR} -lmap-sampler-pmpi -lmap-sampler -Wl,--eh-frame-hd ${ECBUILD_C_LINK_FLAGS}" )
    set( CMAKE_C_LINK_FLAGS "-dynamic -L${ALLINEA_LIBS_DIR} -lmap-sampler-pmpi -lmap-sampler -Wl,--eh-frame-hd ${CMAKE_C_LINK_FLAGS}" )
endif()

# Configuration of the kronos project
# This is uniquely contained as a python module (so that the python doesn't need to be configured to be used/tested).

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/kronos-config-enquire read_file_multiplicity
                OUTPUT_VARIABLE KRONOS_READ_FILE_MULTIPLICITY )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/kronos-config-enquire read_file_size_min
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MIN )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/kronos-config-enquire read_file_size_max
                OUTPUT_VARIABLE KRONOS_READ_SIZE_MAX )
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/kronos-config-enquire write_file_max_chunk
                OUTPUT_VARIABLE KRONOS_WRITE_FILE_MAX_CHUNK )
                
# And build the C components

ecbuild_declare_project()

### export package to other ecbuild packages

set( KRONOS_INCLUDE_DIRS   ${CMAKE_CURRENT_SOURCE_DIR}/kronos-synapps ${CMAKE_CURRENT_BINARY_DIR}/kronos-synapps )
set( KRONOS_LIBRARIES      "" )

get_directory_property( KRONOS_DEFINITIONS COMPILE_DEFINITIONS )

### include directories

include_directories( ${KRONOS_INCLUDE_DIRS} )

add_subdirectory( kronos-synapps )

### finalize project

### Choice between Modeller+executor or executor only

# copy README and HISTORY to the build folder
file( COPY README.rst DESTINATION . )
file( COPY HISTORY.rst DESTINATION . )
file( COPY MANIFEST.in DESTINATION . )

# if building both the executor and the modeller:
if( EXE_AND_MODELLER )

file( COPY conda_environment_exe.txt DESTINATION . )
file( COPY conda_environment.txt DESTINATION . )

set( PYTHON_SCRIPTS 
"
        'bin/kronos-ingest',
        'bin/kronos-model',
        'bin/kronos-view-json',
        'bin/kronos-show-info',
        'bin/kronos-config',
        'bin/kronos-config-exe',
        'bin/kronos-kpf',
        'bin/kronos-krf',
        'bin/kronos-ksf',
        'bin/kronos-config-enquire',
        'bin/kronos-dummy-workload',
        'bin/kronos-generate-read-files',
        'bin/kronos-executor',
        'bin/kronos-raw-to-kpf',
        'bin/kronos-plot-kpf',
        'bin/kronos-map-2-kpf',
        'bin/kronos-dsh-2-kpf',
        'bin/kronos-krf-2-kpf',
        'bin/kronos-run-perf',
        'bin/kronos-run-collect',
        'bin/kronos-run-analysis',
"
)

else()

file( COPY conda_environment_exe.txt DESTINATION . )

file( COPY
     bin/kronos-view-json
     bin/kronos-show-info
     bin/kronos-config-exe
     bin/kronos-kpf
     bin/kronos-krf
     bin/kronos-ksf
     bin/kronos-config-enquire
     bin/kronos-generate-read-files
     bin/kronos-executor
     bin/kronos-run-perf
     bin/kronos-run-collect
     DESTINATION bin )

ecbuild_add_resources( TARGET top_files_1
                       SOURCES_DONT_PACK 
                       docs 
                       dev
                       kronos/core 
                       conda_environment.txt
                       "bin/kronos-config$"
                       bin/kronos-dummy-workload                                             
                       bin/kronos-dsh-2-kpf
                       bin/kronos-krf-2-kpf
                       bin/kronos-ingest
                       bin/kronos-map-2-kpf 
                       bin/kronos-model                       
                       bin/kronos-background-load
                       bin/kronos-ksf-info
                       bin/kronos-plot-kpf
                       bin/kronos-raw-to-kpf
                       bin/kronos-run-analysis
                    )
                    
set( PYTHON_SCRIPTS 
"
        'bin/kronos-view-json',
        'bin/kronos-show-info',        
        'bin/kronos-config-exe',
        'bin/kronos-kpf',
        'bin/kronos-krf',
        'bin/kronos-ksf',
        'bin/kronos-config-enquire',        
        'bin/kronos-generate-read-files',
        'bin/kronos-executor',               
        'bin/kronos-run-perf',
        'bin/kronos-run-collect',
"
)

endif()


### configure the setup.py
file( COPY kronos DESTINATION . )

configure_file( setup.py.in setup.py )


ecbuild_pkgconfig(NAME kronos-synapps
                  DESCRIPTION "Execute synthetic workloads on HPC systems using synthetic applications" )

ecbuild_install_project( NAME ${PROJECT_NAME} )

ecbuild_print_summary()

