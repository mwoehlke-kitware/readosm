include(FindPackageHandleStandardArgs)

find_path(expat_INCLUDE_DIR expat.h ${expat_FIND_OPTS})
find_library(expat_LIBRARY expat ${expat_FIND_OPTS})

find_package_handle_standard_args(
  expat REQUIRED_VARS expat_LIBRARY expat_INCLUDE_DIR)

if(expat_FOUND)
  if(NOT TARGET expat::expat)
    add_library(expat::expat UNKNOWN IMPORTED)
    set_target_properties(expat::expat PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${expat_INCLUDE_DIR}")

    if(EXISTS "${expat_LIBRARY}")
      set_target_properties(expat::expat PROPERTIES
        IMPORTED_LINK_INTERFACE_LANGUAGES "C"
        IMPORTED_LOCATION "${expat_LIBRARY}")
    endif()
  endif()
endif()
