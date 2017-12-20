#ifndef RECOGNIZER_SERVER_RECOGNIZE_H
#define RECOGNIZER_SERVER_RECOGNIZE_H

#define MAX_LOOKUP_TEXT_LEN 10000
#define MAX_IDENTIFIERS 20

typedef struct res_metadata {
    uint8_t type[128];
    uint8_t title[MAX_TITLE_LEN + 1];
    uint8_t authors[MAX_AUTHORS_LEN + 1];
    uint8_t doi[1024];
    uint8_t isbn[14];
    uint8_t arxiv[256];
    uint8_t abstract[10000];
    uint8_t container[512];
    uint8_t publisher[512];
    uint8_t year[5];
    uint8_t pages[32];
    uint8_t volume[32];
    uint8_t issue[32];
    uint8_t issn[32];
} res_metadata_t;

typedef struct pdf_metadata {
    uint8_t title[1024];
    uint8_t doi[1024];
    uint8_t authors[32768];
} pdf_metadata_t;

uint32_t recognize(uint8_t *hash, uint8_t *text, res_metadata_t *result);
uint32_t recognize2(json_t *body, res_metadata_t *result);
int test_authors();

#endif //RECOGNIZER_SERVER_RECOGNIZE_H
