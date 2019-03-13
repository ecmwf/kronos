
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


ecbuild_add_library(

    TARGET network

    SOURCES
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/bool.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/network.c
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/message.h
        ${CMAKE_SOURCE_DIR}/kronos_apps/common/network/message.c

    DEFINITIONS

    GENERATED

    INCLUDES

    LIBS

)


add_subdirectory(tests)
