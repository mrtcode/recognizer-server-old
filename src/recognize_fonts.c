#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <jansson.h>
#include <math.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <unicode/uregex.h>
#include "defines.h"
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "log.h"
#include "wordlist.h"
#include "journal.h"
#include "recognize_fonts.h"

void increment_font(fonts_info_t *fonts_info, uint32_t font_id, double font_size, uint32_t value) {
    for (uint32_t i = 0; i < fonts_info->fonts_len; i++) {
        font_t *font = &fonts_info->fonts[i];
        if (font->font_id == font_id && font->font_size == font_size) {
            font->count += value;
            return;
        }
    }

    fonts_info->fonts_len++;

    if (fonts_info->fonts_len == 1) {
        fonts_info->fonts = (font_t *) malloc(sizeof(font_t) * fonts_info->fonts_len);
    } else {
        fonts_info->fonts = (font_t *) realloc(fonts_info->fonts, sizeof(font_t) * fonts_info->fonts_len);
    }

    fonts_info->fonts[fonts_info->fonts_len - 1].font_id = font_id;
    fonts_info->fonts[fonts_info->fonts_len - 1].font_size = font_size;
    fonts_info->fonts[fonts_info->fonts_len - 1].count = value;
}

int init_fonts_info(fonts_info_t *fonts_info, page_t *pages, uint32_t pages_len) {

    fonts_info->fonts_len = 0;

    for (uint32_t page_i = 0; page_i < pages_len; page_i++) {
        page_t *page = pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        increment_font(fonts_info, word->font, word->font_size, word->text_len);
                    }
                }
            }
        }
    }

    return 1;
}

void print_fonts_info(fonts_info_t *fonts_info) {
    log_debug("fonts:\n");
    for (uint32_t i = 0; i < fonts_info->fonts_len; i++) {
        font_t *font = &fonts_info->fonts[i];
        log_debug("%f %u\n", font->font_size, font->count);
    }
    log_debug("\n");
}

font_t *get_main_font(fonts_info_t *fonts_info) {
    font_t *main_font = 0;
    for (uint32_t i = 0; i < fonts_info->fonts_len; i++) {
        font_t *font = &fonts_info->fonts[i];
        if (!main_font || main_font->count < font->count) {
            main_font = font;
        }
    }
    return main_font;
}
