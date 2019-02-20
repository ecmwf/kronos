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

