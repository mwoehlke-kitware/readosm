cmake_minimum_required(VERSION 3.12)

#------------------------------------------------------------------------------
# Project configuration
#------------------------------------------------------------------------------

project(ReadOSM VERSION 1.1.0)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(GenerateExportHeader)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(expat REQUIRED)
find_package(ZLIB REQUIRED)

# Try to use upstream exported targets first
if(DEFINED protobuf_DIR)
	find_package(protobuf CONFIG REQUIRED)
else()
	find_package(protobuf CONFIG QUIET)
endif()
if(NOT protobuf_FOUND)
  unset(protobuf_DIR CACHE)
  find_package(Protobuf MODULE)
endif()

option(BUILD_SHARED_LIBS
  "Whether to build shared (rather than static) libraries"
  ON
  )

#------------------------------------------------------------------------------
# Build rules
#------------------------------------------------------------------------------

add_library(readosm
  src/osm_objects.c
  src/osmxml.c
  src/protobuf.c
  src/readosm.c
  )

generate_export_header(readosm)

target_compile_definitions(readosm
  PRIVATE
  READOSM_VERSION=\"${PROJECT_VERSION}\"
  )
target_include_directories(readosm
  PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/headers>
  $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  )

target_link_libraries(readosm
  PRIVATE
  ${expat_LIBRARY}
  protobuf::libprotobuf
  ZLIB::ZLIB
  )

#------------------------------------------------------------------------------
# Build-tree export rules
#------------------------------------------------------------------------------

write_basic_package_version_file(
  ${PROJECT_NAME}ConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMinorVersion
  )

configure_file(
  cmake/${PROJECT_NAME}Config.cmake.in
  ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
  @ONLY
  )

export(TARGETS readosm
  NAMESPACE ${PROJECT_NAME}::
  FILE ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake
  )

#------------------------------------------------------------------------------
# Install rules
#------------------------------------------------------------------------------

install(TARGETS readosm
  EXPORT ${PROJECT_NAME}_EXPORTS
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT Runtime
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Runtime
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT Development
  )

install(FILES headers/readosm.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  COMPONENT Development
  )

install(FILES
  ${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
  ${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
  COMPONENT Development
  )

install(EXPORT ${PROJECT_NAME}_EXPORTS
  FILE ${PROJECT_NAME}Targets.cmake
  NAMESPACE ${PROJECT_NAME}::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
  COMPONENT Development
  )

install(SCRIPT
  CODE "
    set(READOSM_VERSION ${READOSM_VERSION})
    set(CMAKE_INSTALL_LIBDIR ${CMAKE_INSTALL_LIBDIR})
    set(CMAKE_INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR})
    configure_file(
      \"${PROJECT_SOURCE_DIR}/readosm.pc.in\"
      \"${PROJECT_BINARY_DIR}/readosm.pc\"
      )
  "
  COMPONENT Development
  )

install(FILES
  ${PROJECT_BINARY_DIR}/readosm.pc
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
  COMPONENT Development
  )

#------------------------------------------------------------------------------
# Unit tests
#------------------------------------------------------------------------------

option(BUILD_TESTING "Whether to enable unit tests" ON)
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()
