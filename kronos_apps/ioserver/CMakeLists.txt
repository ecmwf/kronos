
# -- terminate worker
ecbuild_add_executable(

    TARGET terminate_io_worker

    SOURCES
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/message.h
        terminate_io_worker.c

    LIBS
        msglogger
        network
        kronos
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
        "${PMEMIO_INCLUDE_DIRS}"

    LIBS
        kronos
        msglogger
        network
        "${PMEMIO_OBJ_LIBRARY}"
)


# -- the remote master
ecbuild_add_executable(

    TARGET remote_io_master

    SOURCES
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/message.h
        remote_io_master.c
        io_data.h
        io_data.c

    LIBS
        kronos
        msglogger
        network
)


# Add unit tests directory
add_subdirectory( tests )


# Move the scripts associated with the executables above into the /bin folder
add_custom_command(
        TARGET remote_io_worker POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${PROJECT_SOURCE_DIR}/kronos_apps/scripts/spinup_servers.py
                ${CMAKE_BINARY_DIR}/bin)

add_custom_command(
        TARGET remote_io_worker POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${PROJECT_SOURCE_DIR}/kronos_apps/scripts/spindown_servers.py
                ${CMAKE_BINARY_DIR}/bin)

add_custom_command(
        TARGET remote_io_worker POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${PROJECT_SOURCE_DIR}/kronos_apps/scripts/generate_ioserver_schedule.py
                ${CMAKE_BINARY_DIR}/bin)
