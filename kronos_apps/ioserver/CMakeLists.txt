
if( PMEMIO_FOUND )
    set(PMEMIO_DEFINITIONS "HAVE_PMEMIO")
endif()


# -- terminate worker
ecbuild_add_executable(

    TARGET terminate_io_worker

    SOURCES
        terminate_io_worker.c

    DEFINITIONS        

    INCLUDES

    LIBS
        msglogger
)


# -- the remote worker
ecbuild_add_executable(

    TARGET remote_io_worker

    SOURCES
        io_executor.h
        io_executor.c
        io_task.h
        io_task.c
        nvram_layout.h
        remote_io_worker.c

    DEFINITIONS
        "${PMEMIO_DEFINITIONS}"

    INCLUDES

    LIBS
        kronos
        msglogger
)


# -- the remote master
ecbuild_add_executable(

    TARGET remote_io_master

    SOURCES
        network.h
        network.c
        remote_io_master.c

    DEFINITIONS

    INCLUDES

    LIBS
        kronos
        msglogger
)

ecbuild_add_library (

    TARGET ioserver_network

    SOURCES
        network.h
        network.c

    DEFINITIONS

    GENERATED

    INCLUDES

    LIBS
)


# Add unit tests directory
add_subdirectory( tests )


add_custom_command(
        TARGET remote_io_worker POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/kronos_apps/scripts/spinup_servers.py
                ${CMAKE_BINARY_DIR}/bin)

add_custom_command(
        TARGET remote_io_worker POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/kronos_apps/scripts/spindown_servers.py
                ${CMAKE_BINARY_DIR}/bin)

