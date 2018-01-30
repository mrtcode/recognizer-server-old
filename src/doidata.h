#ifndef RECOGNIZER_SERVER_DOIDATA_H
#define RECOGNIZER_SERVER_DOIDATA_H

#include "defines.h"

typedef struct doidata {
    uint8_t author1_len;
    uint32_t author1_hash;
    uint8_t author2_len;
    uint32_t author2_hash;
    uint8_t doi[DOI_LEN];
} doidata_t;

uint32_t doidata_init(char *directory);

uint32_t doidata_get(uint64_t title_hash, doidata_t *doidatas, uint32_t *doidatas_len);

uint32_t doidata_close();

#endif //RECOGNIZER_SERVER_DOIDATA_H
