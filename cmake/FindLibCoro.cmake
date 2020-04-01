find_path(LIBCORO_INCLUDE_DIR NAMES coro.h)
find_library(LIBCORO_LIBRARIES NAMES coro)

if(LIBCORO_INCLUDE_DIR AND LIBCORO_LIBRARIES)
    set(LIBCORO_FOUND ON)
endif(LIBCORO_INCLUDE_DIR AND LIBCORO_LIBRARIES)


if(LIBCORO_FOUND)
    if (NOT LIBCORO_FIND_QUIETLY)
        message(STATUS "Found libcoro includes: ${LIBCORO_INCLUDE_DIR}/coro.h")
        message(STATUS "Found libcoro library: ${LIBCORO_LIBRARIES}")
    endif (NOT LIBCORO_FIND_QUIETLY)
else(LIBCORO_FOUND)
    if (LIBCORO_FIND_REQUIRED)
        message(FATAL_ERROR "Could not find libcoro development files")
    endif (LIBCORO_FIND_REQUIRED)
endif (LIBCORO_FOUND)

