#cmake_minimum_required(VERSION 3.8)
project(recognizer-server)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES src/main.c src/ht.c src/db.c src/xxhash.c src/text.c src/recognize.c src/recognize.h src/index.c src/index.h src/rh.c src/dedup.c)
add_executable(recognizer-server ${SOURCE_FILES})

set(CMAKE_C_FLAGS_RELEASE "-O2")

target_link_libraries(recognizer-server icuio icui18n icuuc icudata onion sqlite3 jansson pthread jemalloc)