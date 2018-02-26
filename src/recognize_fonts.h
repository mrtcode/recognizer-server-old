
#ifndef RECOGNIZER_SERVER_RECOGNIZE_FONTS_H
#define RECOGNIZER_SERVER_RECOGNIZE_FONTS_H

typedef struct font {
    uint32_t font_id;
    double font_size;
    uint32_t count;
} font_t;

typedef struct fonts_info {
    font_t *fonts;
    uint32_t fonts_len;
} fonts_info_t;

uint32_t init_fonts_info(fonts_info_t *fonts_info, page_t *pages, uint32_t pages_len);

void print_fonts_info(fonts_info_t *fonts_info);

font_t *get_main_font(fonts_info_t *fonts_info);

#endif //RECOGNIZER_SERVER_RECOGNIZE_FONTS_H
