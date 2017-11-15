#ifndef RECOGNIZER_SERVER_BLACKLIST_H
#define RECOGNIZER_SERVER_BLACKLIST_H

#define BLACKLIST_ERROR 0
#define BLACKLIST_SUCCESS 1

int blacklist_init();

uint8_t blacklist_add(uint64_t h);

uint8_t blacklist_has(uint64_t h);

#endif //RECOGNIZER_SERVER_BLACKLIST_H
