
if( HAVE_MPI )
    set(MPI_DEFINITIONS "HAVE_MPI")
endif()

if( HAVE_PMEMIO )
    set(PMEMIO_DEFINITIONS "HAVE_PMEMIO")
endif()

#message("MPI_C_DEFINITIONS have been defined: ${MPI_C_DEFINITIONS}")
#message("MPI_C_INCLUDE_PATH have been defined: ${MPI_C_INCLUDE_PATH}")
#message("MPI_C_LIBRARIES have been defined: ${MPI_C_LIBRARIES}")
#message("\n")
#message("PMEMIO_DEFINITIONS have been defined: ${PMEMIO_DEFINITIONS}")
#message("PMEMIO_INCLUDE_DIRS have been defined: ${PMEMIO_INCLUDE_DIRS}")
#message("PMEMIO_LIBRARIES have been defined: ${PMEMIO_LIBRARIES}")


set(PMEMIO_LIBS "")
list(APPEND PMEMIO_LIBS)


configure_file( kronos_version.h.in kronos_version.h )
configure_file( kronos_version.c.in kronos_version.c )

ecbuild_add_library (

    TARGET kronos

    SOURCES
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/bool.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/json.c
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/json.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/utility.c
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/utility.h
        configure_read_files.h
        configure_write_files.h
        kronos_version.h
        kronos_version.c
        cpu.c
        cpu.h
        memory.c
        memory.h
        memory_persist.c
        memory_persist.h
        file_read.c
        file_read.h
        file_write.c
        file_write.h
        frames.c
        frames.h
        fs_metadata.c
        fs_metadata.h
        global_config.c
        global_config.h
        kernels.c
        kernels.h
        mpi_kernel.c
        mpi_kernel.h
        stats.c
        stats.h
        trace.c
        trace.h
        notification.c
        notification.h

    DEFINITIONS
        "${MPI_DEFINITIONS}"
        "${PMEMIO_DEFINITIONS}"

    GENERATED
        kronos_version.h
        kronos_version.c

    INCLUDES
        "${MPI_C_INCLUDE_PATH}"
        "${PMEMIO_INCLUDE_DIRS}"

    LIBS
        m
        "${MPI_C_LIBRARIES}"
        "${PMEMIO_OBJ_LIBRARY}"
        )


ecbuild_add_executable(

    TARGET kronos-synapp

    SOURCES synapp.c

    DEFINITIONS
        "${MPI_DEFINITIONS}"
        "${PMEMIO_DEFINITIONS}"

    INCLUDES
        "${MPI_C_INCLUDE_PATH}"
        "${PMEMIO_INCLUDE_DIRS}"

    LIBS
        kronos
        "${MPI_C_LIBRARIES}"
        "${PMEMIO_OBJ_LIBRARY}"
        )

file( RELATIVE_PATH _tgt_path ${PROJECT_SOURCE_DIR} ${CMAKE_CURRENT_LIST_DIR} )

configure_file( configure_read_files.h.in ${PROJECT_BINARY_DIR}/${_tgt_path}/configure_read_files.h @ONLY )

configure_file( configure_write_files.h.in ${PROJECT_BINARY_DIR}/${_tgt_path}/configure_write_files.h @ONLY )

# include the tests
add_subdirectory( tests )

# move the appropriate script into the bin directory..
add_custom_command(
        TARGET kronos-synapp POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${PROJECT_SOURCE_DIR}/kronos_apps/scripts/send_job_complete_msg.py
                ${PROJECT_BINARY_DIR}/bin)
