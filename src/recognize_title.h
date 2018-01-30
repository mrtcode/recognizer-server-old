
#ifndef RECOGNIZER_SERVER_RECOGNIZE_TITLE_H
#define RECOGNIZER_SERVER_RECOGNIZE_TITLE_H

typedef struct title_blocks {
    block_t *blocks[1000];
    uint32_t blocks_len;
    uint32_t text_len;
    double max_font_size;
    double x_min;
    double x_max;
    double y_min;
    double y_max;
    double sq;
    uint8_t upper;
} title_blocks_t;

uint32_t get_title_blocks(page_t *page, title_blocks_t *title_blocks, uint32_t *title_blocks_len, uint32_t title_blocks_size);

uint32_t get_doi_by_title(uint8_t *title, uint8_t *processed_text, uint32_t processed_text_len, uint8_t *doi);

#endif //RECOGNIZER_SERVER_RECOGNIZE_TITLE_H
