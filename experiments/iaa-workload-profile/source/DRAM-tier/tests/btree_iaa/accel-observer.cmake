# Additional test translation unit, without editing the official checkout.
set(SPLIT_ACCEL_OBSERVER "${CMAKE_CURRENT_LIST_DIR}/accel_observer.cpp" CACHE FILEPATH "Split experiment observer")
cmake_language(DEFER CALL target_sources zlib-accel PRIVATE "${SPLIT_ACCEL_OBSERVER}")
cmake_language(DEFER CALL target_include_directories zlib-accel PRIVATE "${CMAKE_SOURCE_DIR}")
