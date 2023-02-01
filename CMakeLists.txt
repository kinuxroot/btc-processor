cmake_minimum_required(VERSION 3.16)

project(btc-processor-posix)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

function (add_common_deps TGT)
    target_include_directories(
        ${TGT}
        PRIVATE
        include
    )

    target_sources(
        ${TGT}
        PRIVATE
        src/logging/formatters/CFormatter.cpp
        src/utils/io_utils.cpp
        src/utils/io_utils.cpp
        src/fmt/format.cc
        src/fmt/ostream.cc
        src/fmt/printf.cc
        include/btc-config.h
        include/logging/Formatter.h
        include/logging/Handler.h
        include/logging/Level.h
        include/logging/Logger.h
        include/logging/Record.h
        include/logging/formatters/CFormatter.h
        include/logging/handlers/DefaultHandler.h
        include/logging/handlers/FileHandler.h
        include/logging/handlers/StreamHandler.h
        include/fmt/container.h
        include/fmt/format.h
        include/fmt/ostream.h
        include/fmt/printf.h
        include/fmt/string.h
        include/fmt/time.h
    )
endfunction()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      add_compile_options(-pthread)
      add_link_options(-pthread)
endif()

add_executable(
    btc_combine_blocks
    src/btc_blocks_combiner/btc_blocks_combiner.cpp
    src/btc_blocks_combiner/logger.cpp
)
target_sources(
    btc_combine_blocks
    PRIVATE
    include/btc_blocks_combiner/logger.h
)
add_common_deps(btc_combine_blocks)

add_executable(
    btc_gen_address
    "src/btc_gen_address/btc_gen_address.cpp"
    src/btc_gen_address/logger.cpp
)
target_sources(
    btc_gen_address
    PRIVATE
    include/btc_gen_address/logger.h
)
add_common_deps(btc_gen_address)
