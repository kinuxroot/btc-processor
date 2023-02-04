cmake_minimum_required(VERSION 3.16)

project(btc-processor-posix)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(JSON_BuildTests OFF CACHE INTERNAL "")

add_subdirectory(nlohmann_json)

function (add_executable_deps TGT)
    target_include_directories(
        ${TGT}
        PRIVATE
        include
    )

    target_sources(
        ${TGT}
        PRIVATE
        include/btc-config.h
        include/btc-config.sample.h
    )

    target_link_libraries(${TGT} logging)
    target_link_libraries(${TGT} utils)
    target_link_libraries(${TGT} fmt)
endfunction()

function (add_library_deps TGT)
    target_include_directories(
        ${TGT}
        PRIVATE
        include
    )

    target_sources(
        ${TGT}
        PRIVATE
        include/btc-config.h
        include/btc-config.sample.h
    )
endfunction()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      add_compile_options(-pthread)
      add_link_options(-pthread)
endif()

add_library(
    logging
    src/logging/formatters/CFormatter.cpp
)
target_sources(
    logging
    PRIVATE
    include/logging/Formatter.h
    include/logging/Handler.h
    include/logging/Level.h
    include/logging/Logger.h
    include/logging/Record.h
    include/logging/formatters/CFormatter.h
    include/logging/handlers/DefaultHandler.h
    include/logging/handlers/FileHandler.h
    include/logging/handlers/StreamHandler.h
)
add_library_deps(logging)

add_library(
    utils
    src/utils/io_utils.cpp
    src/utils/task_utils.cpp
    src/utils/json_utils.cpp
    src/utils/btc_utils.cpp
    src/utils/mem_utils.cpp
    src/utils/union_find.cpp
)
target_sources(
    utils
    PRIVATE
    include/utils/io_utils.h
    include/utils/task_utils.h
    include/utils/json_utils.h
    include/utils/btc_utils.h
    include/utils/mem_utils.h
    include/utils/union_find.h
)
add_library_deps(utils)
target_link_libraries(utils nlohmann_json::nlohmann_json)

add_library(
    fmt
    src/fmt/format.cc
    src/fmt/ostream.cc
    src/fmt/printf.cc
)
target_sources(
    fmt
    PRIVATE
    include/fmt/container.h
    include/fmt/format.h
    include/fmt/ostream.h
    include/fmt/printf.h
    include/fmt/string.h
    include/fmt/time.h
)
add_library_deps(fmt)

add_executable(
    btc_combine_blocks
    src/btc_combine_blocks/main.cpp
    src/btc_combine_blocks/logger.cpp
)
target_sources(
    btc_combine_blocks
    PRIVATE
    include/btc_combine_blocks/logger.h
)
add_executable_deps(btc_combine_blocks)

add_executable(
    btc_gen_address
    src/btc_gen_address/main.cpp
    src/btc_gen_address/logger.cpp
)
target_sources(
    btc_gen_address
    PRIVATE
    include/btc_gen_address/logger.h
)
add_executable_deps(btc_gen_address)
target_link_libraries(btc_gen_address nlohmann_json::nlohmann_json)

add_executable(
    btc_combine_address
    src/btc_combine_address/main.cpp
    src/btc_combine_address/logger.cpp
)
target_sources(
    btc_combine_address
    PRIVATE
    include/btc_combine_address/logger.h
)
add_executable_deps(btc_combine_address)

add_executable(
    btc_gen_day_ins
    src/btc_gen_day_ins/main.cpp
    src/btc_gen_day_ins/logger.cpp
)
target_sources(
    btc_gen_day_ins
    PRIVATE
    include/btc_gen_day_ins/logger.h
)
add_executable_deps(btc_gen_day_ins)
target_link_libraries(btc_gen_day_ins nlohmann_json::nlohmann_json)

add_executable(
    btc_union_find
    src/btc_union_find/main.cpp
    src/btc_union_find/logger.cpp
)
target_sources(
    btc_union_find
    PRIVATE
    include/btc_union_find/logger.h
)
add_executable_deps(btc_union_find)
target_link_libraries(btc_union_find nlohmann_json::nlohmann_json)

install(TARGETS btc_combine_blocks btc_combine_address btc_gen_address btc_gen_day_ins)
