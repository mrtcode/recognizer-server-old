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
#include "recognize_abstract.h"
#include "recognize_authors.h"
#include "recognize_fonts.h"
#include "recognize_jstor.h"
#include "recognize_pages.h"
#include "recognize_title.h"
#include "recognize_various.h"

extern UNormalizer2 *unorm2;

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

        page->content_x_left=9999999;
        page->content_x_right=0;

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


                        if(page->content_x_left>word->xMin) page->content_x_left = word->xMin;
                        if(page->content_x_right<word->xMax) page->content_x_right = word->xMax;
                    }
                }
            }
        }
    }
    return doc;
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

uint32_t get_first_page_by_fonts(doc_t *doc) {

    uint32_t start_page = 0;

    uint32_t fonts[100][100];
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

    if(doc->pages_len<2) return 0;

    for (int i = 0; i < doc->pages_len - 2; i++) {
        if (doc->pages[i].width != doc->pages[i + 1].width && doc->pages[i + 1].width == doc->pages[i + 2].width) {
            first_page = i + 1;
        }
    }

    return first_page;
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

int extract_from_headfoot(doc_t *doc, uint8_t *journal, uint8_t *volume, uint8_t *issue, uint8_t *year) {
    uint8_t text[8192] = {0};
    extract_header_footer(doc, text, sizeof(text));

    log_info("HEADFOOT: %s\n", text);

    extract_volume(text, volume);
    extract_issue(text, issue);
    extract_year(text, year);
    extract_journal(text, journal);
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

    if(!processed_text_len) return 0;

    extract_doi(text, result->doi);
    extract_isbn(text, result->isbn);
    extract_arxiv(text, result->arxiv);
    extract_issn(text, result->issn);

    if (!*result->doi) {
        if (strlen(pdf_metadata.title)) {
            if (get_doi_by_title(pdf_metadata.title, processed_text, processed_text_len, result->doi)) {
                strcpy(result->title, pdf_metadata.title);
            }
        }
    }

    uint32_t first_page = 0;

    // Abstract extraction is another way to get the first page
    for (uint32_t i = 0; i < doc->pages_len; i++) {
        page_t *pg = &doc->pages[i];
        if (
                extract_abstract_structured(pg, result->abstract, sizeof(result->abstract)) ||
                extract_abstract_simple(pg, result->abstract, sizeof(result->abstract))) {
            first_page = i;
            log_info("abstract found in page index %d\n", first_page);


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

        log_info("PAGES: total: %d, start: %d, first: %d, last: %d\n", total_pages, start, first, last);
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
//    print_fonts_info(&fonts_info);

    extract_from_headfoot(doc, result->container, result->volume, result->issue, result->year);

    title_blocks_t title_blocks[50];
    uint32_t title_blocks_len = 0;
    get_title_blocks(page, title_blocks, &title_blocks_len, 50);

    log_info("largest font size: %f\n", title_blocks[0].max_font_size);

    title_blocks_t *max_gb = &title_blocks[0];
    for (uint32_t i = 0; i < title_blocks_len; i++) {
        title_blocks_t *gb1 = &title_blocks[i];
        if (gb1->sq > max_gb->sq) max_gb = gb1;
    }

    double title_font_size = 0;

    for (uint32_t i = 0; i < title_blocks_len; i++) {
        title_blocks_t *gb = &title_blocks[i];

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
        log_info("possible title: %s\n", title);

        // Measure characters count instead of byte len
        // "Biometric Encryption™" ?
        if (title_len < 20 || title_len > 500) continue;

        if (!*result->doi) {
            get_doi_by_title(title, processed_text, processed_text_len, result->doi);
        }

        if (get_alphabetic_percent(title) < 70) continue;

        uint8_t authors[10000] = {0};
        get_authors(gb->yMax, page, authors, sizeof(authors));
        log_info("possible authors: %s\n", authors);

        if (title_len >= 20 && title_font_size < gb->max_font_size && *authors) {
            strcpy(result->title, title);
            strcpy(result->authors, authors);
            title_font_size = gb->max_font_size;
        }
    }

    return 0;
}
