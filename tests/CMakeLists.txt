function(readosm_add_test NAME)
  add_executable(${NAME} ${NAME}.c)
  target_link_libraries(${NAME} readosm)
  add_test(NAME ${NAME}
    COMMAND ${NAME}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endfunction()

readosm_add_test(check_err)
readosm_add_test(check_osm)
readosm_add_test(check_pbf)
