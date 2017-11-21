#ifndef RECOGNIZER_SERVER_RECOGNIZE_H
#define RECOGNIZER_SERVER_RECOGNIZE_H

#define MAX_LOOKUP_TEXT_LEN 10000
#define MAX_IDENTIFIERS 20

typedef struct title_metrics {
    uint32_t len;
    uint32_t offset;
    uint32_t distance;
    uint32_t inner_nls;
    uint32_t outer_nls;
    uint32_t in_quotes;
} title_metrics_t;

typedef struct authors_metrics {
    uint32_t offset;
    uint32_t len;
} authors_metrics_t;

typedef struct res_metadata {
    uint64_t metadata_hash;
    uint8_t title[MAX_TITLE_LEN + 1];
    uint8_t authors[MAX_AUTHORS_LEN + 1];
    uint8_t abstract[MAX_ABSTRACT_LEN + 1];
    uint16_t year;
    int32_t year_offset;
    uint8_t identifiers[MAX_IDENTIFIERS][MAX_IDENTIFIER_LEN + 1];
    uint32_t identifiers_len;
} res_metadata_t;

typedef struct result {
    res_metadata_t metadata;
    uint32_t detected_titles;
    uint32_t detected_abstracts;
    uint32_t detected_metadata_through_title;
    uint32_t detected_metadata_through_abstract;
    uint32_t detected_metadata_through_hash;
} result_t;

uint32_t recognize(uint8_t *hash, uint8_t *text, result_t *result);
uint32_t recognize2(uint8_t *hash, json_t *body, result_t *result);

#endif //RECOGNIZER_SERVER_RECOGNIZE_H
