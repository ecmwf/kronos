
#-- logger library
ecbuild_add_library(

    TARGET msglogger

    SOURCES
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/logger.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/logger.c

    DEFINITIONS

    INCLUDES

    LIBS
)


add_subdirectory(tests)
