
#ifndef RECOGNIZER_SERVER_RECOGNIZE_TITLE_H
#define RECOGNIZER_SERVER_RECOGNIZE_TITLE_H

typedef struct line_block {
    line_t *lines[1000];
    uint32_t lines_len;
    uint32_t char_len;
    double max_font_size;
    double x_min;
    double x_max;
    double y_min;
    double y_max;
    double sq;
    uint8_t upper;
    uint8_t bold;
    uint32_t dominating_font;
    uint8_t centered;
} line_block_t;

uint32_t print_block(line_block_t *gb);

uint32_t get_doi_by_title(uint8_t *title, uint8_t *processed_text, uint32_t processed_text_len, uint8_t *doi);

uint32_t get_line_blocks(page_t *page, line_block_t *line_blocks, uint32_t *line_blocks_len);

uint32_t extract_title_author(page_t *page, uint8_t *title, uint8_t *authors_str);

uint32_t line_block_to_text(line_block_t *line_block, uint32_t from, uint8_t *text, uint32_t *text_len, uint32_t max_text_size);

#endif //RECOGNIZER_SERVER_RECOGNIZE_TITLE_H
