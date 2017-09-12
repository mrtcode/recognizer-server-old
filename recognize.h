#ifndef RECOGNIZER_SERVER_RECOGNIZE_H
#define RECOGNIZER_SERVER_RECOGNIZE_H

#define MAX_LOOKUP_TEXT_LEN 10000

typedef struct res_metadata {
    uint64_t title_hash;
    uint8_t title[MAX_TITLE_LEN + 1];
    uint8_t authors[MAX_AUTHORS_LEN + 1];
    uint8_t abstract[MAX_ABSTRACT_LEN + 1];
    uint16_t year;
    int32_t year_offset;
} res_metadata_t;

typedef struct res_identifier {
    uint64_t title_hash;
    uint8_t type;
    uint8_t str[MAX_IDENTIFIER_LEN + 1];
} res_identifier_t;

typedef struct result {
    res_metadata_t metadata;
    res_identifier_t identifiers[100];
    uint32_t identifiers_len;
    uint32_t detected_titles;
    uint32_t detected_abstracts;
    uint32_t detected_titles_through_abstract;
    uint32_t detected_titles_through_hash;
} result_t;

uint32_t recognize(uint8_t *hash, uint8_t *text, result_t *result);

#endif //RECOGNIZER_SERVER_RECOGNIZE_H
