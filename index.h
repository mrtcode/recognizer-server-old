#ifndef RECOGNIZER_SERVER_INDEX_H
#define RECOGNIZER_SERVER_INDEX_H

#define MAX_TITLE_LEN 1023
#define MAX_AUTHORS_LEN 8191
#define MAX_ABSTRACT_LEN 8191
#define MAX_IDENTIFIER_LEN 255
#define HASHABLE_ABSTRACT_LEN 96
#define MAX_IDENTIFIERS_PER_ITEM 3

typedef struct metadata {
    uint8_t *title;
    uint8_t *authors;
    uint8_t *abstract;
    uint8_t *year;
    uint8_t *identifiers;
    uint8_t *hash;
} metadata_t;

time_t index_updated_t();

uint64_t index_total_indexed();

uint32_t index_metadata(metadata_t *metadata);

#endif //RECOGNIZER_SERVER_INDEX_H
