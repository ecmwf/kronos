
if( HAVE_MPI )
    set(MPI_DEFINITIONS "HAVE_MPI")
endif()

configure_file( kronos_version.h.in kronos_version.h )
configure_file( kronos_version.c.in kronos_version.c )

ecbuild_add_library (

    TARGET kronos

    SOURCES
        bool.h
        configure_read_files.h
        configure_write_files.h
        kronos_version.h
        kronos_version.c
        cpu.c
        cpu.h
        memory.c
        memory.h
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
        json.c
        json.h
        kernels.c
        kernels.h
        mpi_kernel.c
        mpi_kernel.h
        stats.c
        stats.h
        trace.c
        trace.h
        utility.c
        utility.h
        notification.c
        notification.h

    DEFINITIONS "${MPI_DEFINITIONS}"

    GENERATED
        kronos_version.h
        kronos_version.c

    INCLUDES "${MPI_C_INCLUDE_PATH}"

    LIBS m "${MPI_C_LIBRARIES}" )


ecbuild_add_executable(

    TARGET kronos-coordinator

    SOURCES coordinator.c

    DEFINITIONS "${MPI_DEFINITIONS}"

    INCLUDES "${MPI_C_INCLUDE_PATH}"

    LIBS kronos "${MPI_C_LIBRARIES}" )

file( RELATIVE_PATH _tgt_path ${CMAKE_SOURCE_DIR} ${CMAKE_CURRENT_LIST_DIR} )

configure_file( configure_read_files.h.in ${CMAKE_BINARY_DIR}/${_tgt_path}/configure_read_files.h @ONLY )

configure_file( configure_write_files.h.in ${CMAKE_BINARY_DIR}/${_tgt_path}/configure_write_files.h @ONLY )


