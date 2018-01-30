#cmake_minimum_required(VERSION 3.8)
project(recognizer-server)

set(CMAKE_C_STANDARD 99)

set(SOURCE_FILES
        src/main.c
        src/doidata.c
        src/xxhash.c
        src/text.c
        src/recognize.c
        src/recognize.h
        src/word.c
        src/journal.c
        src/log.h
        src/recognize_abstract.c
        src/recognize_authors.c
        src/recognize_fonts.c
        src/recognize_jstor.c
        src/recognize_pages.c
        src/recognize_title.c
        src/recognize_various.c
        )
add_executable(recognizer-server ${SOURCE_FILES})

set(CMAKE_C_FLAGS_RELEASE "-O2")

target_link_libraries(recognizer-server icuio icui18n icuuc icudata onion sqlite3 jansson pthread jemalloc z)