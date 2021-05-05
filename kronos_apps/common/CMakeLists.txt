
#-- logger library
ecbuild_add_library(

    TARGET msglogger

    SOURCES
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/logger.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/logger.c

    PRIVATE_INCLUDES
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/kronos_apps>
)


ecbuild_add_library(

    TARGET network

    SOURCES
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/bool.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/network.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/network.c
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/message.h
        ${PROJECT_SOURCE_DIR}/kronos_apps/common/network/message.c

    PRIVATE_INCLUDES
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/kronos_apps>
)


add_subdirectory(tests)
