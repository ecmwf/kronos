
if( PMEMIO_FOUND )
    set(PMEMIO_DEFINITIONS "HAVE_PMEMIO")
endif()


# -- terminate worker
ecbuild_add_executable(

    TARGET terminate_io_worker

    SOURCES
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/message.h
        terminate_io_worker.c

    DEFINITIONS        

    INCLUDES

    LIBS
        msglogger
        network
)


# -- the remote worker
ecbuild_add_executable(

    TARGET remote_io_worker

    SOURCES
        io_data.h
        io_data.c
        io_task.h
        io_task_read.h
        io_task_read_nv.h
        io_task_write.h
        io_task_write_nv.h
        io_task.c
        io_task_read.c
        io_task_read_nv.c
        io_task_write.c
        io_task_write_nv.c
        nvram_layout.h
        remote_io_worker.c

    DEFINITIONS
        "${PMEMIO_DEFINITIONS}"

    INCLUDES

    LIBS
        kronos
        msglogger
        network
)


# -- the remote master
ecbuild_add_executable(

    TARGET remote_io_master

    SOURCES
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/message.h
        remote_io_master.c

    DEFINITIONS

    INCLUDES

    LIBS
        kronos
        msglogger
        network
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

