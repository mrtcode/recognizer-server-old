#cmake_minimum_required(VERSION 3.8)
project(recognizer-server)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES main.c ht.c db.c xxhash.c text.c recognize.c recognize.h index.c index.h rh.c dedup.c)
add_executable(recognizer-server ${SOURCE_FILES})

set(CMAKE_C_FLAGS_RELEASE "-O2")

target_link_libraries(recognizer-server icuio icui18n icuuc icudata onion sqlite3 jansson pthread jemalloc)