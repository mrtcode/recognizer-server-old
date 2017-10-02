#ifndef RECOGNIZER_SERVER_DEDUP_H
#define RECOGNIZER_SERVER_DEDUP_H

#define DEDUP_ERROR 0
#define DEDUP_SUCCESS 1
#define DEDUP_DUPLICATED 2


int dedup_init();

uint8_t dedup_fields(uint64_t mh, uint64_t dh);

uint8_t dedup_hmh(uint8_t type, uint64_t h, uint64_t mh);

#endif //RECOGNIZER_SERVER_DEDUP_H
