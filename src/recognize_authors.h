#ifndef RECOGNIZER_SERVER_RECOGNIZE_AUTHORS_H
#define RECOGNIZER_SERVER_RECOGNIZE_AUTHORS_H

#include "recognize_title.h"

typedef struct uchar {
    int32_t c;
    word_t *word;
} uchar_t;

typedef struct author {
    uint8_t names[4][128];
    uint32_t names_len;
    double font_size;
    uint32_t font;
    uint8_t ref;
    uint8_t prefix;
    uint8_t last_upper;
} author_t;

uint32_t line_to_uchars(line_t *line, uchar_t *uchars, uint32_t *uchars_len, uint32_t uchars_size);

uint32_t extract_authors_from_line(uchar_t *ustr, uint32_t ustr_len, author_t *authors, uint32_t *authors_len);

uint32_t get_authors2(line_block_t *line_block, uint8_t *authors_str, int authors_str_max_len);

#endif //RECOGNIZER_SERVER_RECOGNIZE_AUTHORS_H
