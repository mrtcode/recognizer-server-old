#ifndef RECOGNIZER_SERVER_DEDUP_H
#define RECOGNIZER_SERVER_DEDUP_H

int dedup_init();

uint8_t dedup_fields(uint64_t th, uint64_t dh);

uint8_t dedup_fhth(uint64_t fh, uint64_t th);

uint8_t dedup_ahth(uint64_t fh, uint64_t th);

#endif //RECOGNIZER_SERVER_DEDUP_H
