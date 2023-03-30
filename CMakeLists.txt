cmake_minimum_required(VERSION 3.16)

project(btc-processor-posix)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(JSON_BuildTests OFF CACHE INTERNAL "")
if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    add_compile_options(/utf-8)
endif()


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
    src/utils/numeric_utils.cpp
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
    include/utils/numeric_utils.h
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
    btc_convert_blocks
    src/btc_convert_blocks/main.cpp
    src/btc_convert_blocks/logger.cpp
)
target_sources(
    btc_convert_blocks
    PRIVATE
    include/btc_convert_blocks/logger.h
)
add_executable_deps(btc_convert_blocks)
target_link_libraries(btc_convert_blocks nlohmann_json::nlohmann_json)

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

add_executable(
    btc_merge_union_find
    src/btc_merge_union_find/main.cpp
    src/btc_merge_union_find/logger.cpp
)
target_sources(
    btc_merge_union_find
    PRIVATE
    include/btc_merge_union_find/logger.h
)
add_executable_deps(btc_merge_union_find)
target_link_libraries(btc_merge_union_find nlohmann_json::nlohmann_json)

add_executable(
    btc_collect_day_ins
    src/btc_collect_day_ins/main.cpp
    src/btc_collect_day_ins/logger.cpp
)
target_sources(
    btc_collect_day_ins
    PRIVATE
    include/btc_collect_day_ins/logger.h
)
add_executable_deps(btc_collect_day_ins)
target_link_libraries(btc_collect_day_ins nlohmann_json::nlohmann_json)

add_executable(
    btc_export_union_find
    src/btc_export_union_find/main.cpp
    src/btc_export_union_find/logger.cpp
)
target_sources(
    btc_export_union_find
    PRIVATE
    include/btc_export_union_find/logger.h
)
add_executable_deps(btc_export_union_find)
target_link_libraries(btc_export_union_find nlohmann_json::nlohmann_json)

add_executable(
    btc_textify_union_find
    src/btc_textify_union_find/main.cpp
    src/btc_textify_union_find/logger.cpp
)
target_sources(
    btc_textify_union_find
    PRIVATE
    include/btc_textify_union_find/logger.h
)
add_executable_deps(btc_textify_union_find)

add_executable(
    btc_gen_address_balance
    src/btc_gen_address_balance/main.cpp
    src/btc_gen_address_balance/logger.cpp
)
target_sources(
    btc_gen_address_balance
    PRIVATE
    include/btc_gen_address_balance/logger.h
)
add_executable_deps(btc_gen_address_balance)
target_link_libraries(btc_gen_address_balance nlohmann_json::nlohmann_json)

add_executable(
    btc_filter_address_balance
    src/btc_filter_address_balance/main.cpp
    src/btc_filter_address_balance/logger.cpp
)
target_sources(
    btc_filter_address_balance
    PRIVATE
    include/btc_filter_address_balance/logger.h
)
add_executable_deps(btc_filter_address_balance)
target_link_libraries(btc_filter_address_balance nlohmann_json::nlohmann_json)

add_executable(
    btc_convert_exchanges
    src/btc_convert_exchanges/main.cpp
    src/btc_convert_exchanges/logger.cpp
)
target_sources(
    btc_convert_exchanges
    PRIVATE
    include/btc_convert_exchanges/logger.h
)
add_executable_deps(btc_convert_exchanges)
target_link_libraries(btc_convert_exchanges nlohmann_json::nlohmann_json)

add_executable(
    btc_uf_exchanges
    src/btc_uf_exchanges/main.cpp
    src/btc_uf_exchanges/logger.cpp
)
target_sources(
    btc_uf_exchanges
    PRIVATE
    include/btc_uf_exchanges/logger.h
)
add_executable_deps(btc_uf_exchanges)
target_link_libraries(btc_uf_exchanges nlohmann_json::nlohmann_json)

add_executable(
    btc_collect_miner_tx
    src/btc_collect_miner_tx/main.cpp
    src/btc_collect_miner_tx/logger.cpp
)
target_sources(
    btc_collect_miner_tx
    PRIVATE
    include/btc_collect_miner_tx/logger.h
)
add_executable_deps(btc_collect_miner_tx)
target_link_libraries(btc_collect_miner_tx nlohmann_json::nlohmann_json)

add_executable(
    btc_find_miner_tx_flow
    src/btc_find_miner_tx_flow/main.cpp
    src/btc_find_miner_tx_flow/logger.cpp
)
target_sources(
    btc_find_miner_tx_flow
    PRIVATE
    include/btc_find_miner_tx_flow/logger.h
)
add_executable_deps(btc_find_miner_tx_flow)
target_link_libraries(btc_find_miner_tx_flow nlohmann_json::nlohmann_json)

add_executable(
    btc_addr_statistics
    src/btc_addr_statistics/main.cpp
    src/btc_addr_statistics/logger.cpp
)
target_sources(
    btc_addr_statistics
    PRIVATE
    include/btc_addr_statistics/logger.h
)
add_executable_deps(btc_addr_statistics)
target_link_libraries(btc_addr_statistics nlohmann_json::nlohmann_json)

add_executable(
    btc_convert_address_balance
    src/btc_convert_address_balance/main.cpp
    src/btc_convert_address_balance/logger.cpp
)
target_sources(
    btc_convert_address_balance
    PRIVATE
    include/btc_convert_address_balance/logger.h
)
add_executable_deps(btc_convert_address_balance)
target_link_libraries(btc_convert_address_balance nlohmann_json::nlohmann_json)

add_executable(
    btc_gen_entity_distribution
    src/btc_gen_entity_distribution/main.cpp
    src/btc_gen_entity_distribution/logger.cpp
)
target_sources(
    btc_gen_entity_distribution
    PRIVATE
    include/btc_gen_entity_distribution/logger.h
)
add_executable_deps(btc_gen_entity_distribution)

add_executable(
    btc_gen_block_info
    src/btc_gen_block_info/main.cpp
    src/btc_gen_block_info/logger.cpp
)
target_sources(
    btc_gen_block_info
    PRIVATE
    include/btc_gen_block_info/logger.h
)
add_executable_deps(btc_gen_block_info)
target_link_libraries(btc_gen_block_info nlohmann_json::nlohmann_json)

add_executable(
    btc_find_missed_blocks
    src/btc_find_missed_blocks/main.cpp
    src/btc_find_missed_blocks/logger.cpp
)
target_sources(
    btc_find_missed_blocks
    PRIVATE
    include/btc_find_missed_blocks/logger.h
)
add_executable_deps(btc_find_missed_blocks)
target_link_libraries(btc_find_missed_blocks nlohmann_json::nlohmann_json)

add_executable(
    btc_match_exchange_address
    src/btc_match_exchange_address/main.cpp
    src/btc_match_exchange_address/logger.cpp
)
target_sources(
    btc_match_exchange_address
    PRIVATE
    include/btc_match_exchange_address/logger.h
)
add_executable_deps(btc_match_exchange_address)

add_executable(
    btc_gen_address_statistics
    src/btc_gen_address_statistics/main.cpp
    src/btc_gen_address_statistics/logger.cpp
)
target_sources(
    btc_gen_address_statistics
    PRIVATE
    include/btc_gen_address_statistics/logger.h
)
add_executable_deps(btc_gen_address_statistics)
target_link_libraries(btc_gen_address_statistics nlohmann_json::nlohmann_json)

install(TARGETS
    btc_combine_blocks
    btc_combine_address
    btc_gen_address
    btc_convert_blocks
    btc_gen_day_ins
    btc_union_find
    btc_merge_union_find
    btc_collect_day_ins
    btc_export_union_find
    btc_gen_address_balance
    btc_convert_address_balance
    btc_filter_address_balance
    btc_convert_exchanges
    btc_uf_exchanges
    btc_collect_miner_tx
    btc_find_miner_tx_flow
    btc_addr_statistics
    btc_gen_entity_distribution
    btc_gen_block_info
    btc_find_missed_blocks
    btc_textify_union_find
    btc_match_exchange_address
    btc_gen_address_statistics
)
