
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
        remote_io_worker.c

    DEFINITIONS

    INCLUDES

    LIBS
        kronos
        msglogger
)


# -- the remote master
ecbuild_add_executable(

    TARGET remote_io_master

    SOURCES
        remote_io_master.c

    DEFINITIONS

    INCLUDES

    LIBS
        kronos
        msglogger
)


# Add unit tests directory
add_subdirectory( tests )

