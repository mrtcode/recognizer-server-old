#ifndef RECOGNIZER_SERVER_RECOGNIZE_H
#define RECOGNIZER_SERVER_RECOGNIZE_H

#include "defines.h"


typedef struct word {
    double xMin;
    double xMax;
    double yMin;
    double yMax;
    uint8_t space;
    double font_size;
    double baseline;
    uint8_t rotation;
    uint8_t underlined;
    uint8_t bold;
    uint8_t italic;
    uint32_t font;
    uint32_t color;
    uint8_t *text;
    uint32_t text_len;
} word_t;

typedef struct line {
    word_t *words;
    uint32_t words_len;
    double xMin;
    double xMax;
    double yMin;
    double yMax;
} line_t;

typedef struct block {
    uint8_t alignment;
    line_t *lines;
    uint32_t lines_len;
    double xMin;
    double xMax;
    double yMin;
    double yMax;
    double font_size_min;
    double font_size_max;
    uint32_t text_len;
    uint8_t used;
} block_t;

typedef struct flow {
    block_t *blocks;
    uint32_t blocks_len;
} flow_t;

typedef struct page {
    flow_t *flows;
    uint32_t flows_len;
    double width;
    double height;
    double content_x_left;
    double content_x_right;
} page_t;

typedef struct doc {
    page_t *pages;
    uint32_t pages_len;
} doc_t;


typedef struct res_metadata {
    uint8_t type[TYPE_LEN];
    uint8_t title[TITLE_LEN + 1];
    uint8_t authors[AUTHORS_LEN + 1];
    uint8_t doi[DOI_LEN + 1];
    uint8_t isbn[ISBN_LEN + 1];
    uint8_t arxiv[ARXIV_LEN + 1];
    uint8_t abstract[ABSTRACT_LEN + 1];
    uint8_t container[CONTAINER_LEN + 1];
    uint8_t publisher[PUBLISHER_LEN + 1];
    uint8_t year[YEAR_LEN + 1];
    uint8_t pages[PAGES_LEN + 1];
    uint8_t volume[VOLUME_LEN + 1];
    uint8_t issue[ISSUE_LEN + 1];
    uint8_t issn[ISSN_LEN + 1];
    uint8_t url[URL_LEN + 1];
} res_metadata_t;

typedef struct pdf_metadata {
    uint8_t title[TITLE_LEN];
    uint8_t doi[DOI_LEN];
    uint8_t authors[AUTHORS_LEN];
} pdf_metadata_t;

uint32_t recognize(json_t *body, res_metadata_t *result);


#endif //RECOGNIZER_SERVER_RECOGNIZE_H
