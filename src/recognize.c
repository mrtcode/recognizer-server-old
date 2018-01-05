/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2017 Zotero
 https://www.zotero.org

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 ***** END LICENSE BLOCK *****
 */

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

extern UNormalizer2 *unorm2;

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
} page_t;

typedef struct doc {
    page_t *pages;
    uint32_t pages_len;
} doc_t;

doc_t *get_doc(json_t *body) {

    doc_t *doc = (doc_t *) malloc(sizeof(doc_t));

    json_t *json_pages = json_object_get(body, "pages");
    if (!json_is_array(json_pages)) return 0;
    uint32_t pages_len = json_array_size(json_pages);

    doc->pages = (page_t *) malloc(sizeof(page_t) * pages_len);
    doc->pages_len = pages_len;

    for (uint32_t page_i = 0; page_i < pages_len; page_i++) {
        json_t *json_obj = json_array_get(json_pages, page_i);
        page_t *page = doc->pages + page_i;

        json_t *width = json_array_get(json_obj, 0);
        json_t *height = json_array_get(json_obj, 1);

        page->width = json_number_value(width);
        page->height = json_number_value(height);

        json_t *json_flows = json_array_get(json_obj, 2);
        if (!json_is_array(json_flows)) return 0;
        page->flows_len = json_array_size(json_flows);
        page->flows = (flow_t *) malloc(sizeof(flow_t) * page->flows_len);

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            json_t *json_obj = json_array_get(json_flows, flow_i);
            flow_t *flow = page->flows + flow_i;

            json_t *json_blocks = json_array_get(json_obj, 0);
            if (!json_is_array(json_blocks)) return 0;
            flow->blocks_len = json_array_size(json_blocks);
            flow->blocks = (block_t *) calloc(sizeof(block_t), flow->blocks_len);
            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                json_t *json_obj = json_array_get(json_blocks, block_i);
                block_t *block = flow->blocks + block_i;

                block->font_size_min = 0;
                block->font_size_max = 0;

                block->text_len = 0;

                json_t *xMin = json_array_get(json_obj, 0);
                json_t *yMin = json_array_get(json_obj, 1);
                json_t *xMax = json_array_get(json_obj, 2);
                json_t *yMax = json_array_get(json_obj, 3);

                block->xMin = json_number_value(xMin);
                block->xMax = json_number_value(xMax);
                block->yMin = json_number_value(yMin);
                block->yMax = json_number_value(yMax);

                json_t *json_lines = json_array_get(json_obj, 4);
                if (!json_is_array(json_lines)) return 0;
                block->lines_len = json_array_size(json_lines);
                block->lines = (line_t *) calloc(sizeof(line_t), block->lines_len);
                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    json_t *json_obj = json_array_get(json_lines, line_i);
                    line_t *line = block->lines + line_i;

                    json_t *json_words = json_array_get(json_obj, 0);
                    if (!json_is_array(json_words)) return 0;
                    line->words_len = json_array_size(json_words);
                    line->words = (word_t *) malloc(sizeof(word_t) * line->words_len);


                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        json_t *json_obj = json_array_get(json_words, word_i);
                        word_t *word = line->words + word_i;

                        json_t *xMin = json_array_get(json_obj, 0);
                        json_t *yMin = json_array_get(json_obj, 1);
                        json_t *xMax = json_array_get(json_obj, 2);
                        json_t *yMax = json_array_get(json_obj, 3);
                        json_t *fontSize = json_array_get(json_obj, 4);
                        json_t *spaceAfter = json_array_get(json_obj, 5);
                        json_t *baseline = json_array_get(json_obj, 6);
                        json_t *rotation = json_array_get(json_obj, 7);
                        json_t *underlined = json_array_get(json_obj, 8);
                        json_t *bold = json_array_get(json_obj, 9);
                        json_t *italic = json_array_get(json_obj, 10);
                        json_t *color = json_array_get(json_obj, 11);
                        json_t *font = json_array_get(json_obj, 12);
                        json_t *text = json_array_get(json_obj, 13);


                        word->xMin = json_number_value(xMin);
                        word->xMax = json_number_value(xMax);
                        word->yMin = json_number_value(yMin);
                        word->yMax = json_number_value(yMax);
                        word->font_size = json_number_value(fontSize);
                        word->space = json_integer_value(spaceAfter);
                        word->baseline = json_number_value(baseline);
                        word->rotation = json_integer_value(rotation);
                        word->underlined = json_integer_value(rotation);
                        word->bold = json_integer_value(bold);
                        word->italic = json_integer_value(italic);
                        word->color = json_integer_value(color);
                        word->font = json_integer_value(font);

                        word->text = json_string_value(text);
                        word->text_len = strlen(word->text);

                        if (block->font_size_min == 0 || block->font_size_min > word->font_size) {
                            block->font_size_min = word->font_size;
                        }

                        if (block->font_size_max < word->font_size) {
                            block->font_size_max = word->font_size;
                        }

                        block->text_len += word->text_len;

                        if (!line->xMin || line->xMin > word->xMin) line->xMin = word->xMin;
                        if (!line->yMin || line->yMin > word->yMin) line->yMin = word->yMin;
                        if (line->xMax < word->xMax) line->xMax = word->xMax;
                        if (line->yMax < word->yMax) line->yMax = word->yMax;

                    }
                }
            }
        }
    }
    return doc;
}

typedef struct grouped_blocks {
    block_t *blocks[1000];
    uint32_t blocks_len;
    uint32_t text_len;
    double max_font_size;
    double xMin;
    double xMax;
    double yMin;
    double yMax;
    double sq;
    uint8_t upper;
} grouped_blocks_t;

int is_block_upper(block_t *block) {
    for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
        line_t *line = block->lines + line_i;

        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
            word_t *word = line->words + word_i;

            text_info_t text_info = text_get_info(word->text);
            if (text_info.lowercase) return 0;
        }
    }
    return 1;
}

int get_groups(page_t *page, grouped_blocks_t *grouped_blocks, uint32_t *grouped_blocks_len) {
    grouped_blocks_t gb_max;
    double gb_max_font_size = 0;

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;
            if (block->used) continue;

            if (block->yMin <= 50) continue;

            if (block->lines[0].words[0].rotation != 0) continue;

            double max_font_size = block->font_size_max;

            grouped_blocks_t gb;
            memset(&gb, 0, sizeof(grouped_blocks_t));

            gb.upper = is_block_upper(block);

            gb.xMin = block->xMin;
            gb.xMax = block->xMax;
            gb.yMin = block->yMin;
            gb.yMax = block->yMax;

            gb.blocks[gb.blocks_len] = block;
            gb.text_len += block->text_len;

            gb.blocks_len++;

            for (uint32_t flow2_i = 0; flow2_i < page->flows_len; flow2_i++) {
                flow_t *flow2 = page->flows + flow2_i;

                for (uint32_t block2_i = 0; block2_i < flow2->blocks_len; block2_i++) {
                    block_t *block2 = flow2->blocks + block2_i;

                    if (block2->used) continue;
                    if (block2->yMin <= 50) continue;
                    if (flow2_i == flow_i && block2_i == block_i) continue;
                    if ((!is_block_upper(block) && !is_block_upper(block2) ||
                         is_block_upper(block) && is_block_upper(block2)) &&
                        fabs(block->lines[0].words[0].font_size - block2->lines[0].words[0].font_size) < 0.5) {

                        if (
                                gb.blocks[gb.blocks_len - 1]->yMin - block2->yMax > block->font_size_max * 2 ||
                                block2->yMin - gb.blocks[gb.blocks_len - 1]->yMax > block->font_size_max * 2 ||
                                block->lines[0].words[0].bold != block2->lines[0].words[0].bold) {
                            continue;
                        }

//                        if (gb.xMin - block2->xMax > 20 || block2->xMax - gb.xMax > 20) continue;


                        if (!(block->xMin + 10 >= block2->xMin && block->xMax - 10 <= block2->xMax ||
                              block2->xMin + 10 >= block->xMin && block2->xMax - 10 <= block->xMax)) {
                            continue;
                        }


                        if (block2->xMin < gb.xMin) gb.xMin = block2->xMin;
                        if (block2->xMax > gb.xMax) gb.xMax = block2->xMax;
                        if (block2->yMin < gb.yMin) gb.yMin = block2->yMin;
                        if (block2->yMax > gb.yMax) gb.yMax = block2->yMax;

                        if (block2->font_size_max > max_font_size) {
                            max_font_size = block2->font_size_max;
                        }
                        gb.blocks[gb.blocks_len] = block2;
                        gb.text_len += block2->text_len;
                        gb.blocks_len++;

                        block2->used = 1;
                    }
                }
            }

            printf("BL: %s\n", block->lines[0].words[0].text);

            gb.max_font_size = max_font_size;
            grouped_blocks[*grouped_blocks_len] = gb;
            (*grouped_blocks_len)++;
        }
    }
}

int block_to_text(block_t *block, uint8_t *text, uint32_t *text_len, uint32_t max_text_size) {
    *text_len = 0;
    for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
        line_t *line = block->lines + line_i;

        if (line_i != 0) {
            *(text + *text_len) = ' ';
            (*text_len)++;
        }

        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
            word_t *word = line->words + word_i;

            if ((*text_len) + word->text_len >= max_text_size) return 0;
            memcpy(text + *text_len, word->text, word->text_len);
            (*text_len) += word->text_len;

            if (word->space) {
                *(text + *text_len) = ' ';
                (*text_len)++;
            }
        }
    }
    *(text + *text_len) = 0;
    return 1;
}

int doc_to_text(doc_t *doc, uint8_t *text, uint32_t *text_len, uint32_t max_text_size) {
    *text_len = 0;

    for (uint32_t page_i = 0; page_i < doc->pages_len && page_i < 2; page_i++) {
        page_t *page = doc->pages + page_i;
        //printf("PAGE: %f %f\n", page->width, page->height);

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;
            //printf(" FLOW:\n");

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;

//                    if (line_i != 0) {
//                        *(text + *text_len) = ' ';
//                        (*text_len)++;
//                    }

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        if ((*text_len) + word->text_len >= max_text_size) {
                            *(text + *text_len) = 0;
                            return 1;
                        }
                        memcpy(text + *text_len, word->text, word->text_len);
                        (*text_len) += word->text_len;

                        if (word->space) {
                            *(text + *text_len) = ' ';
                            (*text_len)++;
                        }
                    }

                    *(text + *text_len) = '\n';
                    (*text_len)++;
                }

                *(text + *text_len) = '\n';
                (*text_len)++;

            }
        }
    }
    *(text + *text_len) = 0;

    return 1;
}

typedef struct font {
    uint32_t font_id;
    double font_size;
    uint32_t count;
} font_t;

typedef struct fonts_info {
    font_t *fonts;
    uint32_t fonts_len;
} fonts_info_t;

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
    printf("FONTS:\n");
    for (uint32_t i = 0; i < fonts_info->fonts_len; i++) {
        font_t *font = &fonts_info->fonts[i];
        printf("%f %u\n", font->font_size, font->count);
    }
    printf("\n");
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

typedef struct uchar {
    int32_t c;
    word_t *word;
} uchar_t;

int line_to_uchars(line_t *line, uchar_t *uchars, uint32_t *uchars_len) {

    *uchars_len = 0;

    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
        word_t *word = line->words + word_i;
        uint8_t *text = word->text;

        uint32_t s, i = 0;
        UChar32 c;

        do {
            s = i;
            U8_NEXT(text, i, -1, c);

            if (!c) break;

            if (u_charType(c) == U_NON_SPACING_MARK && *uchars_len >= 1) {
                int32_t c_prev = uchars[*uchars_len - 1].c;
                int32_t c_combined = unorm2_composePair(unorm2, c_prev, c);
                if (c_combined) {
                    uchars[*uchars_len - 1].c = c_combined;
                    continue;
                }
            }

            if (i > 500) break;

            uchars[*uchars_len].word = word;
            uchars[*uchars_len].c = c;

            (*uchars_len)++;

        } while (1);

        if (word->space) {
            uchars[*uchars_len].word = word;
            uchars[*uchars_len].c = ' ';
            (*uchars_len)++;
        }

    }
    return 0;
}


int get_word_type(uint8_t *name) {
    int a = 0, b = 0, c = 0;

    uint8_t output_text[300];
    uint32_t output_text_len = 200;

    text_process(name, output_text, &output_text_len);
//
//
    uint64_t word_hash = text_hash64(output_text, output_text_len);
    wordlist_get(word_hash, &a, &b, &c);
//printf("%u %u %u\n", a, b, c);
    if (b + c == 0) return -a;
    if (a == 0) return b + c;

    if (b + c > a) {
        return (b + c) / a;
    } else if (b + c < a)
        return -a / (b + c);
    else return 0;
    //printf("%s %u %u %u\n", name, a, b, c);
}

int is_conjunction(uint8_t *name) {
    static uint8_t names[10][32] = {
            "and", "AND",
            "und", ""
    };
}

typedef struct author {
    uint8_t names[4][128];
    uint32_t names_len;
    double font_size;
    uint32_t fontName;
    uint8_t bold;
    int32_t score;
    double xMin, xMax, yMin, yMax;
    uint8_t ref;
    uint8_t last_upper;
} author_t;

// Check if other authors have the same font size and bold as the first one
int get_authors2(uchar_t *ustr, uint32_t ustr_len, author_t *authors, uint32_t *authors_len) {
    UBool error = 0;

    uint8_t names[4][128];
    uint8_t names_len = 0;
    uint32_t cur_name_len = 0;
    uint8_t n = 0;
    double font_size = 0;
    double baseline = 0;

    int32_t first_character = 0;


    for (uint32_t i = 0; i < ustr_len; i++) {
        uchar_t *uchar = &ustr[i];

//
//            u_printf("%S\n", uchar);
//
//        if(uchar->c=='*')
//            u_printf("%S\n", uchar);
//
//        if(uchar->c=='h')
//            u_printf("%S\n", uchar);

        if(names_len==3 && !strcmp(names[2], "and")) {
            printf("dd: %s\n", names[2]);
        }

        if (!n) {
            authors[*authors_len].font_size = uchar->word->font_size;
            authors[*authors_len].bold = uchar->word->bold;
            authors[*authors_len].xMin = uchar->word->xMin;
            authors[*authors_len].xMax = uchar->word->xMax;
            authors[*authors_len].yMin = uchar->word->yMin;
            authors[*authors_len].yMax = uchar->word->yMax;

            if (u_isUAlphabetic(uchar->c)) {
                //if (u_isUUppercase(uchar->c)) {
                    U8_APPEND(names[names_len], cur_name_len, 100, uchar->c, error);
                    names[names_len][cur_name_len] = 0;
//                    printf("v: %s\n", names[names_len]);
                    if (error) {
                        return 0;
                    }
                    n++;
                    if (names_len == 0) {
                        font_size = ustr->word->font_size;
                        baseline = ustr->word->baseline;
                    }
                //}// else {
//                    //goto end;
//                    return 0;
//                }

                first_character = uchar->c;
            } else if (uchar->c == ' ') {
              continue;
            } else {
                goto end;

            }

        } else {
            if (!u_isUUppercase(first_character) || !strcmp(names[names_len], "and") || !strcmp(names[names_len], "UND")) {
                cur_name_len = 0;
                goto end;
            }

            if (names_len >= 4) {
                goto end;
            }

            if (uchar->word->font_size < font_size && uchar->word->baseline != baseline || uchar->c == '*') {
                authors[*authors_len].ref = 1;
                goto end;
            } else if (u_isUAlphabetic(uchar->c) || uchar->c == '-') {
                U8_APPEND(names[names_len], cur_name_len, 100, uchar->c, error);
                names[names_len][cur_name_len] = 0;
//                printf("v: %s\n", names[names_len]);
                if (error) {
                    return 0;
                }
            } else if (uchar->c == '.' || uchar->c == ' ') {
                names[names_len++][cur_name_len] = 0;
                n = 0;
                cur_name_len = 0;
            } else {
                printf("dd: %s\n", names[0]);
                goto end;
            }
        }

        continue;
        end:
        names[names_len][cur_name_len] = 0;
        if (cur_name_len) {
            names_len++;
        }
        if (names_len >= 2 && names_len <= 4) {
            memcpy(authors[*authors_len].names, names, 4 * 128);
            authors[*authors_len].names_len = names_len;
            (*authors_len)++;
        } else if (names_len != 0)
            return 0;
        n = 0;
        names_len = 0;
        cur_name_len = 0;
    }

    names[names_len][cur_name_len] = 0;
    if (cur_name_len) {
        names_len++;
    }
    if (names_len >= 2 && names_len <= 4) {
        memcpy(authors[*authors_len].names, names, 4 * 128);
        authors[*authors_len].names_len = names_len;
        (*authors_len)++;
    } else
        return 0;

    return 1;
}

int compare_int(const void *b, const void *a) {
    if (((grouped_blocks_t *) a)->max_font_size == ((grouped_blocks_t *) b)->max_font_size) return 0;
    return ((grouped_blocks_t *) a)->max_font_size < ((grouped_blocks_t *) b)->max_font_size ? -1 : 1;
}

int get_authors(double title_yMax, page_t *page, uint8_t *au) {

    uchar_t *ustr = malloc(sizeof(uchar_t) * 1000);
    uint32_t ustr_len;
    author_t *authors = malloc(sizeof(author_t) * 1000);
    uint32_t authors_len = 0;

    ustr_len = 0;

    memset(authors, 0, sizeof(author_t) * 1000);

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            uint8_t started = 0;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                if (line->yMin <= title_yMax) continue;

                started = 1;

                printf("l: %f\n", line->yMin);

                authors_len = 0;

                line_to_uchars(line, ustr, &ustr_len);
                int ret = get_authors2(ustr, ustr_len, authors, &authors_len);

                if (started && authors_len == 0) {
                    free(ustr);
                    free(authors);
                    return 0;
                }

                for (uint32_t j = 0; j < authors_len; j++) {
                    author_t *author = &authors[j];

                    int negative = 0;
                    for (uint32_t k = 0; k < author->names_len; k++) {
                        printf("%s ", author->names[k]);

                        if (strlen(author->names[k]) > 1) {
                            int type = get_word_type(author->names[k]);
                            if (type < 0) negative++;
                            printf(" (%d) ", type);
                        }
                    }

                    if (authors_len == 1 && negative) return 0;

                    if (negative == author->names_len) return 0;

                    for (uint32_t k = 0; k < author->names_len - 1; k++) {
                        strcat(au, author->names[k]);
                        if (k != author->names_len - 2) strcat(au, " ");
                    }

                    strcat(au, "\t");
                    strcat(au, author->names[author->names_len - 1]);
                    strcat(au, "\n");

                    printf("%f %d %f %f %f %f\n", author->font_size, author->bold,
                           author->xMin, author->xMax, author->yMin, author->yMax);
                }
            }
        }
    }

    free(ustr);
    free(authors);

    return 1;
}

uint8_t find_author(uint8_t *text, uint32_t text_len, uint32_t author_hash, uint32_t author_len) {
    for (uint32_t i = 0; i < text_len - author_len; i++) {
        if (author_hash == text_hash32(text + i, author_len)) {
            return 1;
        }
    }
    return 0;
}

int get_doi_by_title(uint8_t *title, uint8_t *processed_text, uint32_t processed_text_len, uint8_t *doi) {
    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
    text_process(title, output_text, &output_text_len);

    uint64_t title_hash = text_hash64(output_text, output_text_len);
    //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);

    if (output_text_len < 14) return 0;
    slot_t *slots[100];
    uint32_t slots_len;

    ht_get_slots(title_hash, slots, &slots_len);
    if (slots_len == 1) {
        for (uint32_t j = 0; j < slots_len; j++) {
            slot_t *slot = slots[j];
            uint8_t author1_found = find_author(processed_text, processed_text_len, slot->a1_hash, slot->a1_len);
            uint8_t author2_found = find_author(processed_text, processed_text_len, slot->a2_hash, slot->a2_len);
            uint8_t res = db_get_doi(slot->doi_id, doi);
            if (res && (author1_found || author2_found)) {
                printf("recognized by title: %s (%d, %d) %s\n", title, author1_found, author2_found, doi);
                return 1;
            }
        }
    }
    return 0;
}

int extract_doi(uint8_t *text, uint8_t *doi) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "10.\\d{4,9}\\/[-._;()\\/:A-Za-z0-9]+";
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    int max_len = 0;

    while (uregex_findNext(regEx, &uStatus)) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        if (end - start > max_len) {
            ucnv_fromUChars(conv, doi, DOI_LEN, uc + start, end - start, &uStatus);
            max_len = end - start;
        }

        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_isbn(uint8_t *text, uint8_t *isbn) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "(SBN|sbn)[ \\u2014\\u2013\\u2012-]?(10|13)?[: ]*([0-9X][0-9X \\u2014\\u2013\\u2012-]+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    uint8_t tmp[32] = {0};
    uint32_t tmp_i = 0;

    if (isMatch) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        for (uint32_t i = start; i <= end; i++) {
            if (uc[i] >= '0' && uc[i] <= '9' || uc[i] == 'X') {
                tmp[tmp_i++] = uc[i];
                if (tmp_i > 13) break;
            }
        }

        if (tmp_i == 10 || tmp_i == 13) {
            strcpy(isbn, tmp);
        }

        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_arxiv(uint8_t *text, uint8_t *arxiv) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "arXiv:([a-z+-]+\\/[a-zA-Z0-9]+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);
    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        ucnv_fromUChars(conv, arxiv, target_len, uc + start, end - start, &uStatus);
        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

uint32_t get_first_page_by_fonts(doc_t *doc) {

    uint32_t start_page = 0;

    uint32_t fonts[10][100];
    uint32_t fonts_len[10] = {0};

    for (uint32_t page_i = 0; page_i < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        uint8_t found = 0;
                        for (uint32_t i = 0; i < fonts_len[page_i]; i++) {
                            if (fonts[page_i][i] == word->font) {
                                found = 1;
                                break;
                            }
                        }

                        if (!found) {
                            fonts[page_i][fonts_len[page_i]++] = word->font;
                        }
                    }
                }
            }
        }
    }

    for (uint32_t page_i = 0; page_i < doc->pages_len - 1; page_i++) {

        uint32_t missing = 0;
        uint32_t total = 0;

        for (uint32_t i = 0; i < fonts_len[page_i]; i++) {
            uint64_t font1 = fonts[page_i][i];


            uint8_t found = 0;

            for (uint32_t page2_i = page_i + 1; page2_i < doc->pages_len; page2_i++) {
                for (uint32_t j = 0; j < fonts_len[page2_i]; j++) {
                    uint64_t font2 = fonts[page2_i][j];
                    total++;

                    if (font1 == font2) {
                        found = 1;
                        break;
                    }
                }
                if (found) break;
            }

            if (!found) {
                missing++;
            }

        }

        if (missing == fonts_len[page_i] && total >= 2) {
            start_page = page_i + 1;
        }
    }

    return start_page;
}

uint32_t get_first_page_by_width(doc_t *doc) {
    uint32_t first_page = 0;

    for (int i = 0; i < doc->pages_len - 2; i++) {
        if (doc->pages[i].width != doc->pages[i + 1].width && doc->pages[i + 1].width == doc->pages[i + 2].width) {
            first_page = i + 1;
        }
    }

    return first_page;
}

uint32_t is_simple_abstract_name(uint8_t *text) {
    static uint8_t names[10][32] = {
            "abstract",
            "summary"
    };

    for (uint32_t i = 0; i < 2; i++) {
        uint8_t *c = names[i];
        uint8_t *v = text;

        uint8_t first = 1;

        while (*c) {
            if (first) {
                if (*c - 32 != *v) break;
            } else {
                if (*c != *v && *c - 32 != *v) break;
            }
            first = 0;
            c++;
            v++;
        }

        if (!*c) {
            printf("FOUND22: %s\n", names[i]);
            return c - names[i];
        }
    }
    return 0;
}

int is_dot_last(uint8_t *text) {
    uint8_t *c = &text[strlen(text) - 1];

    while (c >= text) {
        if (*c == ' ' || *c == '\n' || *c == '\r') {

        } else {
            if (*c == '.') {
                return 1;
            } else {
                return 0;
            }
        }
        c--;
    }

    return 0;
}

int extract_abstract_simple(page_t *page, uint8_t *abstract, uint32_t abstract_size) {
    uint8_t found_abstract = 0;

    uint8_t start = 0;
    uint32_t start_skip = 0;
    uint8_t finish = 0;

    uint32_t abstract_len = 0;

    UBool error = 0;

    double abstract_xMin = 0;
    double abstract_xMax = 0;

    double txt_xMin = 0;
    double txt_xMax = 0;

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                if (!found_abstract) {
                    uint32_t name_len = is_simple_abstract_name(line->words[0].text);
                    if (name_len) {
                        found_abstract = 1;
                        start = 1;
                        start_skip = name_len;
                        abstract_xMin = line->words[0].xMin;
                        abstract_xMax = line->words[0].xMax;
                    }
                }

                if (start) {
                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        uint32_t s, i = 0;
                        UChar32 c;

                        do {
                            s = i;
                            U8_NEXT(word->text, i, -1, c);

                            if (!c) break;

                            if (start_skip) {
                                start_skip--;
                            } else {

                                if (!finish && (u_isUAlphabetic(c) || u_isdigit(c))) {
                                    finish = 1;
                                }

                                if (finish) {

                                    if (!txt_xMin || txt_xMin > word->xMin) {
                                        txt_xMin = word->xMin;
                                    }

                                    if (!txt_xMax || txt_xMax < word->xMax) {
                                        txt_xMax = word->xMax;
                                    }
//                                    if(abstract_xMax && word->xMin > abstract_xMax) {
//                                        return 0;
//                                    }

                                    U8_APPEND(abstract, abstract_len, abstract_size - 1, c, error);
                                    if (error) {
                                        *abstract = 0;
                                        printf("unorm2_getNFKDInstance failed, error=%s", u_errorName(error));
                                        return 0;
                                    }

                                    abstract[abstract_len] = 0;
                                }
                            }
                        } while (1);

                        if (word->space) {
                            if (start_skip) {
                                start_skip--;
                            } else {
                                if (finish) {
                                    if (word->space) {
                                        abstract[abstract_len++] = ' ';
                                        abstract[abstract_len] = 0;
                                    }
                                }
                            }
                        }
                    }
                }

                if (finish) {
                    if (abstract_len && abstract[abstract_len - 1] == '-') {
                        abstract_len--;
                        abstract[abstract_len] = 0;
                    } else {
                        abstract[abstract_len++] = ' ';
                        abstract[abstract_len] = 0;

                    }
                }


                if (finish) {

                    printf("line: %f\n", line->xMax);
                    if (is_dot_last(abstract) &&
                        line_i >= 2 &&
                        fabs(block->lines[line_i - 2].xMax - block->lines[line_i - 1].xMax) < 1.0 &&
                        block->lines[line_i].xMax < block->lines[line_i - 1].xMax - 2) {
                        printf("%s\n\n\n", abstract);
                        abstract[abstract_len] = 0;

                        if (abstract_xMax > txt_xMax || abstract_xMax < txt_xMin) {
                            return 0;
                        }
                        return 1;
                    }
                }
            }

            if (finish) {
//                printf("%s\n\n\n", abstract);
                if (!is_dot_last(abstract)) continue;
                abstract[abstract_len] = 0;

                if (abstract_xMax > txt_xMax || abstract_xMax < txt_xMin) {
                    return 0;
                }
                return 1;
            }
        }

        if (finish) {
            printf("%s\n\n\n", abstract);
            abstract[abstract_len] = 0;

            if (abstract_xMax > txt_xMax || abstract_xMax < txt_xMin) {
                return 0;
            }
            return 1;
        }
    }

    *abstract = 0;
    return 0;
}

uint32_t is_structured_abstract_name(uint8_t *text) {
    static uint8_t names[10][32] = {
            "background",
            "methods",
            "method",
            "conclusions",
            "conclusion",
            "objectives",
            "objective",
            "results",
            "result",
            "purpose",
    };

    uint32_t types[11] = {
            1,
            2,
            2,
            3,
            3,
            4,
            4,
            5,
            5,
            6
    };

    for (uint32_t i = 0; i < 10; i++) {
        uint8_t *c = names[i];
        uint8_t *v = text;

        uint8_t first = 1;

        while (*c) {
            if (first) {
                if (*c - 32 != *v) break;
            } else {
                if (*c != *v && *c - 32 != *v) break;
            }
            first = 0;
            c++;
            v++;
        }

        if (!*c) {
            printf("FOUND11: %s\n", names[i]);
            return types[i];
        }
    }
    return 0;
}

int extract_abstract_structured(page_t *page, uint8_t *abstract, uint32_t abstract_size) {
    uint8_t start = 0;
    uint32_t abstract_len = 0;
    uint8_t exit = 0;
    UBool error = 0;

    uint32_t names_detected = 0;

    double xMin = 0;
    double fontSize = 0;

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                uint32_t type = is_structured_abstract_name(line->words[0].text);
                if (type) {
                    names_detected++;
                    start = 1;
                    exit = 0;
                    if (abstract_len) abstract[abstract_len++] = '\n';
                    printf("\n\n");

                    if (xMin) {
                        if (fabs(xMin - line->words[0].xMin) > 2) {
                            return 0;
                        }
                    } else {
                        xMin = line->words[0].xMin;
                    }

                    if (fontSize) {
                        if (fabs(fontSize - line->words[0].font_size) > 1) {
                            return 0;
                        }
                    } else {
                        fontSize = line->words[0].font_size;
                    }
                }

                if (start) {
                    if (exit) {
                        printf("%s\n\n\n", abstract);
                        *abstract = 0;
                        return 0;
                    }
                }

                if (start) {
                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        uint32_t s, i = 0;
                        UChar32 c;

                        do {
                            s = i;
                            U8_NEXT(word->text, i, -1, c);

                            if (!c) break;

                            U8_APPEND(abstract, abstract_len, abstract_size - 1, c, error);
                            if (error) {
                                *abstract = 0;
                                return 0;
                            }
                        } while (1);

                        if (word->space) {
                            abstract[abstract_len++] = ' ';
                        }

                    }
                }
            }

        }

        if (start) {
            if (names_detected >= 2) {
                printf("%s\n\n\n", abstract);
                abstract[abstract_len] = 0;
                return 1;
            } else {
                *abstract = 0;
                return 0;
            }
        }

    }

    return 0;
}


int extract_year(uint8_t *text, uint8_t *year) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "(^|\\(|\\s)([0-9]{4})(\\)|\\s|$)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    uint8_t tmp[32] = {0};
    uint32_t tmp_i = 0;

    if (isMatch) {
        int32_t start = uregex_start(regEx, 2, &uStatus);
        int32_t end = uregex_end(regEx, 2, &uStatus);

        uint8_t year_str[5]={0};
        int k = 0;
        for (uint32_t i = start; i <= end && k < 4; i++, k++) {
            year_str[k] = uc[i];
        }

        uint32_t year_nr = atoi(year_str);

        if (year_nr >= 1800 && year_nr <= 2018) {
            strcpy(year, year_str);
            ret = 1;
        }
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_volume(uint8_t *text, uint8_t *volume) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);
    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "\\b(?i:volume|vol|v)\\.?[\\s:-]\\s*(\\d+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        if (end - start <= 4) {
            ucnv_fromUChars(conv, volume, VOLUME_LEN, uc + start, end - start, &uStatus);
        }

        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int extract_issue(uint8_t *text, uint8_t *issue) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "\\b(?i:issue|num|no|number|n)\\.?[\\s:-]\\s*(\\d+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        if (end - start <= 4) {
            ucnv_fromUChars(conv, issue, ISSUE_LEN, uc + start, end - start, &uStatus);
        }

        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int extract_pages(doc_t *doc, uint32_t *start, uint32_t *first) {
    for (uint32_t page_i = 0; page_i + 2 < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;
                    if (line->yMax < 100 || line->yMin > page->height - 100) {

                        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                            word_t *word = line->words + word_i;

                            page_t *page2 = doc->pages + page_i + 2;

                            for (uint32_t flow2_i = 0; flow2_i < page2->flows_len; flow2_i++) {
                                flow_t *flow2 = page2->flows + flow2_i;

                                for (uint32_t block2_i = 0; block2_i < flow2->blocks_len; block2_i++) {
                                    block_t *block2 = flow2->blocks + block2_i;

                                    for (uint32_t line2_i = 0; line2_i < block2->lines_len; line2_i++) {
                                        line_t *line2 = block2->lines + line2_i;
                                        if (line2->yMax < 100 || line2->yMin > page2->height - 100) {

                                            for (uint32_t word2_i = 0; word2_i < line2->words_len; word2_i++) {
                                                word_t *word2 = line2->words + word2_i;


                                                if (
                                                        fabs(word->yMin - word2->yMin) < 1.0 &&
                                                        fabs(word->xMin - word2->xMin) < 15.0) {
                                                    //printf("DETECTED: %s %s\n", word->text, word2->text);

                                                    uint8_t *w1;
                                                    uint8_t *w2;

                                                    uint32_t n;

                                                    n = 0;

                                                    int skip = 0;

                                                    for (int i = 0; i < word->text_len && n < 30; i++) {
                                                        if (word->text[i] < '0' || word->text[i] > '9') {
                                                            skip = 1;
                                                            break;
                                                        }
                                                    }

                                                    n = 0;
                                                    for (int i = 0; i < word2->text_len && n < 30; i++) {
                                                        if (word2->text[i] < '0' || word2->text[i] > '9') {
                                                            skip = 1;
                                                            break;
                                                        }
                                                    }

                                                    if (skip) continue;

                                                    w1 = word->text;
                                                    w2 = word2->text;


//                                                    printf("%s %s\n", w1, w2);

                                                    uint32_t nr1 = atoi(w1);
                                                    uint32_t nr2 = atoi(w2);
//                                                    printf("found numbers: %d %d\n", nr1, nr2);
                                                    if (nr1 > 0 && nr2 == nr1 + 1) {
                                                        printf("found numbers: %d %d\n", nr1, nr2);
                                                        *start = page_i;
                                                        *first = nr1;
                                                        return 1;
                                                    }

                                                }


                                            }

                                        }
                                    }

                                }
                            }


                        }

                    }
                }

            }
        }

    }

    return 0;
}

int extract_issn(uint8_t *text, uint8_t *issn) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "ISSN:?\\s*(\\d{4}[-]\\d{3}[\\dX])";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);
    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        ucnv_fromUChars(conv, issn, target_len, uc + start, end - start, &uStatus);
        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int get_block_text(block_t *block, uint8_t *text) {
    uint32_t text_len = 0;

    for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
        line_t *line = block->lines + line_i;

        *(text + text_len) = '\n';
        (text_len)++;
        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
            word_t *word = line->words + word_i;

            if ((text_len) + word->text_len >= 5000) {
                *(text + text_len) = 0;
                return 1;
            }
            memcpy(text + text_len, word->text, word->text_len);
            (text_len) += word->text_len;

            if (word->space) {
                *(text + text_len) = ' ';
                (text_len)++;
            }
        }

        *(text + text_len) = '\n';
        (text_len)++;
    }
}

int extract_header_footer(doc_t *doc, uint8_t *text, uint32_t text_size) {
    for (uint32_t page_i = 0; page_i + 1 < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                // At the very end or top of page can only be an injected text
                if (block->yMin < 15 || block->yMax > page->height - 15) continue;

                for (uint32_t page2_i = page_i + 1; page2_i < doc->pages_len && page2_i <= page_i + 2; page2_i++) {
                    page_t *page2 = doc->pages + page2_i;

                    for (uint32_t flow2_i = 0; flow2_i < page2->flows_len; flow2_i++) {
                        flow_t *flow2 = page2->flows + flow2_i;

                        for (uint32_t block2_i = 0; block2_i < flow2->blocks_len; block2_i++) {
                            block_t *block2 = flow2->blocks + block2_i;

                            double width1 = block->xMax - block->xMin;
                            double height1 = block->yMax - block->yMin;

                            double width2 = block2->xMax - block2->xMin;
                            double height2 = block2->yMax - block2->yMin;

                            if (
                                    fabs(block->xMin - block2->xMin) < 10 &&
                                    fabs(block->yMin - block2->yMin) < 10 &&
                                    fabs(width1 - width2) < 10 &&
                                    fabs(height1 - height2) < 10) {
                                uint8_t data1[10000] = {0};
                                uint8_t data2[10000] = {0};
                                get_block_text(block, data1);
                                get_block_text(block2, data2);

                                if (!strcmp(data1, data2)) {
                                    if (!strstr(text, data1)) {
                                        if (strlen(data1) + strlen(text) < text_size - 1) {
                                            strcat(text, data1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return 0;
}

int extract_journal(uint8_t *text, uint8_t *journal) {
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);
    UConverter *conv = ucnv_open("UTF-8", &errorCode);
    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);
    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "([\\p{Alphabetic}'.]+\\s)*[\\p{Alphabetic}'.]+";
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    while (uregex_findNext(regEx, &uStatus)) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        uint8_t res[2048];
        ucnv_fromUChars(conv, res, sizeof(res), uc + start, end - start, &uStatus);

        uint8_t *s = res;

        uint8_t *e;

        uint32_t tokens_num = 0;

        while (*s) {
            while (*s == ' ') s++;
            e = s;
            while (*e != ' ' && *e != 0) e++;
            tokens_num++;
            s = e;
        }

        if (tokens_num < 2) continue;

        uint8_t processed_res[MAX_LOOKUP_TEXT_LEN];
        uint32_t processed_res_len = MAX_LOOKUP_TEXT_LEN;
        text_process(res, processed_res, &processed_res_len);

        uint64_t title_hash = text_hash64(processed_res, processed_res_len);
        if (journal_has(title_hash)) {
            strcpy(journal, res);
        }
    }

    uregex_close(regEx);
    free(uc);
}

int extract_from_headfoot(doc_t *doc, uint8_t *journal, uint8_t *volume, uint8_t *issue, uint8_t *year) {
    uint8_t text[8192] = {0};
    extract_header_footer(doc, text, sizeof(text));

    printf("HEADFOOT: %s\n", text);

    extract_volume(text, volume);
    extract_issue(text, issue);
    extract_year(text, year);
    extract_journal(text, journal);
}

int extract_jt(uint8_t *text, uint8_t *regText, uint8_t groups[][2048], uint32_t *groups_len) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    if (uregex_find(regEx, 0, &uStatus)) {
        *groups_len = uregex_groupCount(regEx, &uStatus);

        for (uint32_t i = 1; i < *groups_len + 1; i++) {
            int32_t start = uregex_start(regEx, i, &uStatus);
            int32_t end = uregex_end(regEx, i, &uStatus);

            if (end - start < 512) {
                ucnv_fromUChars(conv, groups[i - 1], target_len, uc + start, end - start, &uStatus);
            }
        }
        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int get_jstor_data(page_t *page, uint8_t *text, uint32_t *text_len, uint32_t max_text_size) {

    *text_len = 0;
    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {

        flow_t *flow = &page->flows[flow_i];

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = &flow->blocks[block_i];

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                uint8_t line_str[512] = {0};
                uint32_t line_str_len = 0;

                for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                    word_t *word = line->words + word_i;

                    if (line_str_len + word->text_len > 500) break;

                    memcpy(line_str + line_str_len, word->text, word->text_len);
                    (line_str_len) += word->text_len;

                    if (word->space) {
                        *(line_str + line_str_len) = ' ';
                        (line_str_len)++;
                    }
                }

                if (*text_len + line_str_len > max_text_size - 5) return 0;
                memcpy(text + *text_len, line_str, line_str_len);
                (*text_len) += line_str_len;

                text[(*text_len)++] = '\n';
//                text[++(*text_len)] = 0;

                if (!strncmp(line_str, "Stable URL: http://www.jstor.org/stable/", 40)) {
                    return 1;
                }
            }
            text[(*text_len)++] = '\n';
        }
    }

    return 0;
}

int extract_jstor(page_t *page, res_metadata_t *result) {
    uint8_t authors[2048] = {0};
    uint8_t source[2048] = {0};
    uint8_t published_by[2048] = {0};

    uint8_t text[4096] = {0};
    uint32_t text_len = 0;

    if (!get_jstor_data(page, text, &text_len, sizeof(text))) return 0;

    uint8_t is_book = 0;
    uint8_t *text_start;

    text_start = strstr(text, "Chapter Title: ");

    if (text_start) {
        is_book = 1;
    } else {
        text_start = strstr(text, "\n\n");
        if (text_start) {
            text_start += 2;
        }
    }

    if (!text_start) text_start = text;

    printf("BLOCK TEXT: %s\n", text_start);

    uint8_t groups[5][2048] = {0};
    uint32_t groups_len = 0;

    if (extract_jt(text, "Stable URL: (http:\\/\\/www.\\jstor\\.org\\/stable\\/(\\S+))", groups,
                   &groups_len)) {
        strcpy(result->url, groups[0]);
        sprintf(result->doi, "10.2307/%s", groups[1]);
    } else {
        return 0;
    }

    if (is_book) {
        strcpy(result->type, "book-chapter");
        if (extract_jt(text_start, "Chapter Title: ((?:\\n|.)*)\\nChapter Author\\(s\\): ((?:\\n|.)*)\\n\\n", groups,
                       &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
        } else if (extract_jt(text_start, "Chapter Title: ((?:\\n|.)*)\\n\\n", groups,
                              &groups_len)) {
            strcpy(result->title, groups[0]);
        }

        if (extract_jt(text_start, "Book Title: ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                       &groups_len)) {
            strcpy(result->container, groups[0]);
        }

        if (extract_jt(text_start, "Book Subtitle: ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                       &groups_len)) {
            strcat(result->container, ": ");
            strcat(result->container, groups[0]);
        }

        if (!*authors && extract_jt(text_start, "Book Author\\(s\\): ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                                    &groups_len)) {
            strcpy(authors, groups[0]);
        }

        if (extract_jt(text_start, "Published by: ((?:\\n|.)*?)\\nStable URL: ", groups,
                       &groups_len)) {
            strcat(published_by, groups[0]);
        }
    } else {
        strcpy(result->type, "journal-article");

        if (extract_jt(text_start, "((?:\\n|.)*)\\nAuthor\\(s\\): (.*)\\nReview by: (.*)\\nSource: (.*)\\n", groups,
                       &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[2]);
            strcpy(source, groups[3]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nAuthor\\(s\\): (.*)\\nSource: (.*)\\n", groups,
                              &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
            strcpy(source, groups[2]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nReview by: (.*)\\nSource: (.*)\\n", groups, &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
            strcpy(source, groups[2]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nSource: (.*)\\n", groups, &groups_len)) {
            strcpy(result->title, groups[0]);;
            strcpy(source, groups[1]);
        }
    }

    if (*authors) {
        uint8_t *s = authors;
        uint8_t *e;
        while (1) {
            e = strstr(s, ", ");
            if (e) {
                *e = 0;
                strcat(result->authors, s);
                strcat(result->authors, "\n");
                e++;
                continue;
            }

            e = strstr(s, " and ");
            if (e) {
                *e = 0;
                strcat(result->authors, s);
                strcat(result->authors, "\n");

                e++;
                continue;
            }

            strcat(result->authors, s);
            strcat(result->authors, "\n");

            break;
        }
    }

    uint8_t *vol;
    uint8_t *no;
    uint8_t *pg;

    vol = strstr(source, ", Vol. ");

    if (vol) {
        uint8_t *c = vol + 7;
        uint8_t *v = result->volume;
        while (*c >= '0' && *c <= '9' && v - result->volume < 10) {
            *v++ = *c++;
        }

        if (vol - source < sizeof(result->container) - 1) {
            memcpy(result->container, source, vol - source);
            result->container[vol - source] = 0;
        }
    }

    no = strstr(source, ", No. ");

    if (no) {
        uint8_t *c = no + 6;
        uint8_t *v = result->issue;
        while (*c >= '0' && *c <= '9' && v - result->issue < 10) {
            *v++ = *c++;
        }

        if (!*result->container) {
            if (no - source < sizeof(result->container) - 1) {
                memcpy(result->container, source, no - source);
                result->container[no - source] = 0;
            }
        }
    }

    uint8_t *c = source;
    int32_t source_len = strlen(source);

    while (c - source < source_len - 4) {
        if (
                c[0] >= '0' && c[0] <= '9' &&
                c[1] >= '0' && c[1] <= '9' &&
                c[2] >= '0' && c[2] <= '9' &&
                c[3] >= '0' && c[3] <= '9' &&
                c[4] == ')') {
            memcpy(result->year, c, 4);
            break;
        }
        c++;
    }

    pg = strstr(source, ", p. ");
    if (pg) {
        pg += 5;
    } else {
        pg = strstr(source, ", pp. ");
        if (pg) {
            pg += 6;
        }
    }

    if (pg) {
        strcpy(result->pages, pg);
    }

    if (*published_by) {
        uint32_t len = strlen(published_by);
        uint8_t *c = published_by + len - 1;

        while (*c <= 64 && c >= published_by) {
            c--;
        }

        if (c > published_by) {
            memcpy(result->publisher, published_by, c - published_by + 1);
        }

        c = published_by + len - 5;
        if (
                c[0] >= '0' && c[0] <= '9' &&
                c[1] >= '0' && c[1] <= '9' &&
                c[2] >= '0' && c[2] <= '9' &&
                c[3] >= '0' && c[3] <= '9' &&
                c[4] == ')') {
            memcpy(result->year, c, 4);
        }
    }

    return 1;
}

int process_metadata(json_t *json_metadata, pdf_metadata_t *pdf_metadata) {
    const char *key;
    json_t *value;

    json_object_foreach(json_metadata, key, value) {
        if (json_is_string(value)) {
            if (!strcmp(key, "Title")) {
                uint8_t *title = json_string_value(value);
                if (title && strlen(title) <= TITLE_LEN) {
                    strcpy(pdf_metadata->title, title);
                    return 1;
                }
            }
        }
    }
    return 0;
}

uint32_t recognize(json_t *body, res_metadata_t *result) {
    memset(result, 0, sizeof(res_metadata_t));

    strcpy(result->type, "journal-article");

    json_t *json_metadata = json_object_get(body, "metadata");
    json_t *json_total_pages = json_object_get(body, "totalPages");

    if (!json_metadata || !json_total_pages) return 0;

    uint32_t total_pages = json_integer_value(json_total_pages);

    pdf_metadata_t pdf_metadata = {0};

    if (json_is_object(json_metadata)) {
        process_metadata(json_metadata, &pdf_metadata);
    }

    doc_t *doc = get_doc(body);

    if (extract_jstor(&doc->pages[0], result)) return 1;

    uint8_t text[MAX_LOOKUP_TEXT_LEN];
    uint32_t text_len = MAX_LOOKUP_TEXT_LEN;
    uint8_t processed_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t processed_text_len = MAX_LOOKUP_TEXT_LEN;

    doc_to_text(doc, text, &text_len, MAX_LOOKUP_TEXT_LEN - 1);
    text_process(text, processed_text, &processed_text_len);

    //extract_doi(text, result->doi);
    extract_isbn(text, result->isbn);
    extract_arxiv(text, result->arxiv);
    extract_issn(text, result->issn);

//    if (!*result->doi) {
//        if (strlen(pdf_metadata.title)) {
//            if (get_doi_by_title(pdf_metadata.title, processed_text, processed_text_len, result->doi)) {
//                strcpy(result->title, pdf_metadata.title);
//            }
//        }
//    }

    uint32_t first_page = 0;

    // Abstract extraction is another way to get the first page
    for (uint32_t i = 0; i < doc->pages_len; i++) {
        page_t *pg = &doc->pages[i];
        if (
                extract_abstract_structured(pg, result->abstract, sizeof(result->abstract)) ||
                extract_abstract_simple(pg, result->abstract, sizeof(result->abstract))) {
            first_page = i;
            printf("abstract found in page index %d\n", first_page);


            uint8_t *c = &result->abstract[strlen(result->abstract) - 1];
            while (c >= result->abstract) {
                if (*c == ' ' || *c == '\n' || *c == '\r') {
                    *c = 0;
                } else {
                    break;
                }
                c--;
            }
            if (*result->abstract && result->abstract[strlen(result->abstract) - 1] != '.') {
                *result->abstract = 0;
                first_page = 0;
            }


            break;
        }
    }

    if (!first_page) {
        uint32_t res;

        res = get_first_page_by_width(doc);
        if (res) {
            first_page = res;
        } else {

            res = get_first_page_by_fonts(doc);
            if (res) {
                first_page = res;
            }
        }
    }

    int start;
    int first = 1;
    int last = total_pages;

    if (extract_pages(doc, &start, &first)) {
        if (first == 1) {
            first_page = start;
        } else if (first == 2 && start >= 1) {
            first_page = start - 1;
            first = 1;
        }

        last = first + total_pages - first_page - 1;

        printf("PAGES: total: %d, start: %d, first: %d, last: %d\n", total_pages, start, first, last);
    }

    if (!*result->pages) {
        if (first > 1) {
            sprintf(result->pages, "%d-%d", first, last);
        } else {
            sprintf(result->pages, "%d", last);
        }
    }


    page_t *page = &doc->pages[first_page];

    fonts_info_t fonts_info;
    init_fonts_info(&fonts_info, page, doc->pages_len - first_page);

    font_t *main_font = get_main_font(&fonts_info);
    print_fonts_info(&fonts_info);

    extract_from_headfoot(doc, result->container, result->volume, result->issue, result->year);

    grouped_blocks_t grouped_blocks[1000];
    uint32_t grouped_blocks_len = 0;
    get_groups(page, grouped_blocks, &grouped_blocks_len);

    qsort(grouped_blocks, grouped_blocks_len, sizeof(grouped_blocks_t), compare_int);

    printf("largest: %f\n", grouped_blocks[0].max_font_size);

    grouped_blocks_t *max_gb = &grouped_blocks[0];
    for (uint32_t i = 0; i < grouped_blocks_len; i++) {
        grouped_blocks_t *gb1 = &grouped_blocks[i];
        if (gb1->sq > max_gb->sq) max_gb = gb1;
    }

    double title_font_size = 0;

    for (uint32_t i = 0; i < grouped_blocks_len; i++) {
        grouped_blocks_t *gb = &grouped_blocks[i];

        if (gb->max_font_size <= main_font->font_size && !gb->upper) continue;

        uint8_t title[1024] = {0};
        uint32_t title_len = 0;
        for (uint32_t k = 0; k < gb->blocks_len; k++) {
            if (title_len >= 500) break;
            uint32_t block_title_len;
            block_to_text(gb->blocks[k], title + title_len, &block_title_len, 500 - title_len);
            title_len += block_title_len;
            title[title_len++] = ' ';
            title[title_len] = 0;
        }

        // Measure characters count instead of byte len
        if (title_len < 25 || title_len > 500) continue;

        if (!*result->doi) {
            get_doi_by_title(title, processed_text, processed_text_len, result->doi);
        }

        if (get_alphabetic_percent(title) < 70) continue;

        uint8_t authors[10000] = {0};
        get_authors(gb->yMax, page, authors);
        printf("au: %s\n", authors);

        if (title_len >= 20 && title_font_size < gb->max_font_size && strlen(authors)) {
            strcpy(result->title, title);
            strcpy(result->authors, authors);
            printf("TITLE: %s\n", title);
            printf("AUTHORS: %s\n", authors);
            title_font_size = gb->max_font_size;
        }

        printf("TXT: %s\n", title);
    }

    return 0;
}
