
# -- terminate worker
ecbuild_add_executable(

    TARGET terminate_io_worker

    SOURCES
        logger.h
        logger.c
        terminate_io_worker.c

    DEFINITIONS

    INCLUDES

    LIBS
)


# -- the remote worker
ecbuild_add_executable(

    TARGET remote_io_worker

    SOURCES
        logger.h
        logger.c
        io_task.h
        io_task.c
        remote_io_worker.c

    DEFINITIONS

    INCLUDES

    LIBS kronos
)


# -- the remote master
ecbuild_add_executable(

    TARGET remote_io_master

    SOURCES
        logger.h
        logger.c
        remote_io_master.c

    DEFINITIONS

    INCLUDES

    LIBS kronos
)


add_custom_command(
        TARGET remote_io_master POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/spinup_servers.py
                ${CMAKE_BINARY_DIR}/bin)

add_custom_command(
        TARGET remote_io_master POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_CURRENT_SOURCE_DIR}/spindown_servers.py
                ${CMAKE_BINARY_DIR}/bin)


