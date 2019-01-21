if (NOT DEFINED ENABLE_COUCHBASE OR ENABLE_COUCHBASE)
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/couchbase/c-client")
        message (WARNING "submodule contrib/couchbase/c-client is missing. to fix try run: \n git submodule update --init --recursive")
    elseif (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/couchbase/cpp-wrapper")
        message (WARNING "submodule contrib/couchbase/cpp-wrapper is missing. to fix try run: \n git submodule update --init --recursive")
    else()
        set (COUCHBASE_INCLUDE_DIR
                "${ClickHouse_SOURCE_DIR}/contrib/couchbase/c-client/include/"
                "${ClickHouse_SOURCE_DIR}/contrib/couchbase/cpp-wrapper/include/")
        set (COUCHBASE_LIBRARY couchbase)
        set (USE_COUCHBASE 1)

        message(STATUS "Using couchbase: ${COUCHBASE_LIBRARY}")
    endif()
endif()
