#ifndef RECOGNIZER_SERVER_DEDUP_H
#define RECOGNIZER_SERVER_DEDUP_H

#define DEDUP_ERROR 0
#define DEDUP_SUCCESS 1
#define DEDUP_DUPLICATED 2


int dedup_init();

uint8_t dedup_fields(uint64_t th, uint64_t dh);

uint8_t dedup_fhth(uint64_t fh, uint64_t th);

uint8_t dedup_ahth(uint64_t fh, uint64_t th);

#endif //RECOGNIZER_SERVER_DEDUP_H
