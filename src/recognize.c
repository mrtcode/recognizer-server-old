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
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "log.h"
#include "fuzzysearch.h"
#include "blacklist.h"

#define MAX_MH 10

// Only a very basic implementation
//int match_title(uint8_t *processed_text, uint32_t processed_text_len,
//                uint8_t *data, uint32_t data_len) {
//    uint8_t *p = data + 1;
//    uint8_t *s;
//
//    uint8_t *b;
//
//    uint32_t tokens_total = 0;
//    uint32_t tokens_found = 0;
//
//    while (p - data < data_len - 1) {
//        while (p - data < data_len - 1 && (*p == ' ' || *p == '\t' || *p == '\n')) p++;
//        if (p - data >= data_len - 1) break;
//        s = p;
//        while (p - data <= data_len - 1 && *p != ' ' && *p != '\t' && *p != '\n') p++;
//
//        tokens_total++;
//
//        uint8_t token[256];
//        uint32_t token_len = 256;
//        text_process_fieldn(s, p - s, token, &token_len);
//
//        uint8_t *v = strstr(processed_text, token);
//
//        if (v) tokens_found++;
//    }
//
//    if (tokens_total && tokens_found * 100 / tokens_total >= 70) return 1;
//    return 0;
//}

int match_title(uint32_t *utext, uint32_t utext_len,
                uint8_t *data, uint32_t data_len,
                title_metrics_t *tm) {
    uint8_t *p = data + 1;

    uint32_t len = data_len - 1;

    if (utext_len > 4096) utext_len = 4096;

    uint32_t utitle[MAX_LOOKUP_TEXT_LEN];
    uint32_t utitle_len;

    text_process2(data + 1, utitle, &utitle_len, data_len - 1);

    int ret;
    uint32_t offset;
    uint32_t distance;

    uint32_t max_distance;

    if (len <= 30) {
        max_distance = 0;
    } else if (len <= 60) {
        max_distance = 5;
    } else {
        max_distance = 10;
    }

    ret = fuzzysearch(utitle, utitle_len, utext, utext_len, &offset, &distance, max_distance);

    if (!ret) return 0;

    tm->len = len;
    tm->offset = offset;
    tm->distance = distance;

    return 1;
}

int compare_titles(title_metrics_t *a, title_metrics_t *b) {
    uint32_t matched_characters_a = a->len - a->distance;
    uint32_t matched_characters_b = b->len - b->distance;
    if (matched_characters_a > matched_characters_b) {
        return 1;
    }
    return 0;
}

int match_authors(uint8_t *processed_text, uint8_t *data, uint32_t data_len) {
    uint8_t *p = data + 1;
    uint8_t *s;

    uint8_t *first_name, *last_name;

    uint32_t authors_total = 0;
    uint32_t authors_found = 0;

    while (p - data < data_len - 1) {
        while (p - data < data_len - 1 && (*p == '\t' || *p == '\n')) p++;
        if (p - data >= data_len - 1) break;
        s = p;
        while (p - data <= data_len - 1 && *p != '\t' && *p != '\n') p++;

        if (*p == '\t') {
            first_name = s;
        } else {
            authors_total++;
            last_name = s;
            uint8_t norm_last_name[64];
            uint32_t norm_last_name_len = 64;
            text_process_fieldn(last_name, p - s, norm_last_name, &norm_last_name_len);

            uint8_t *v = strstr(processed_text, norm_last_name);

            if (v) authors_found++;
        }
    }

    if (authors_total && authors_found * 100 / authors_total >= 70) return 1;
    return 0;
}

int extract_abstract(uint8_t *text, uint32_t text_len,
                     uint8_t *original_text, uint32_t original_text_len,
                     uint32_t *map, uint32_t map_len,
                     uint8_t *data, uint32_t data_len,
                     uint8_t *output_text, uint32_t *output_text_len) {

    uint16_t len = *((uint16_t *) (data + 1));
    uint32_t xx_hash = *((uint32_t *) (data + 3));
    uint32_t rolling_hash = *((uint32_t *) (data + 7));

    if (!len) return 0;

    uint8_t *p = text;

    uint32_t text_len_rem = text_len;
    while (text_len_rem >= len && (p = text_rh_find32(p, text_len_rem, rolling_hash, len))) {
        if (text_hash32(p, len) == xx_hash) {
            text_raw_abstract(original_text, map, map_len, p - text, p - text + len - 1, output_text, 10000);
            return 1;
        }
        p++;
        text_len_rem = text + text_len - p;
    }

    return 0;
}

int32_t get_year_offset(uint8_t *original_text, uint32_t original_text_len, uint16_t year) {
    uint8_t year_str[5];
    snprintf(year_str, 5, "%u", year);

    uint8_t *p = original_text;
    while (p < original_text + original_text_len - 4) {
        if (!memcmp(p, year_str, 4)) return p - original_text;
        p++;
    }
    return -1;
}

int process_metadata(result_t *result, uint8_t *text, uint64_t metadata_hash,
                     uint8_t *output_text, uint32_t output_text_len,
                     uint32_t *map, uint32_t map_len,
                     uint32_t *output_utext, uint32_t output_utext_len) {
    int ret;

    sqlite3_stmt *stmt = db_get_fields_stmt(metadata_hash);

    uint8_t *data;
    uint32_t data_len;

    res_metadata_t metadata = {0};

    metadata.metadata_hash = metadata_hash;

    title_metrics_t best_tm;
    memset(&best_tm, 0, sizeof(title_metrics_t));

    sqlite3_stmt *stmt2 = db_get_doidata_stmt(metadata_hash);
    while (db_get_next_doidata(stmt2, &data, &data_len)) {
        uint8_t *p = data;
        uint16_t title_len = *((uint16_t *) p);
        p += 2;
        uint8_t *title = p;
        p += title_len;
        p++;
        uint16_t authors_len = *((uint16_t *) p);
        p += 2;
        uint8_t *authors = p;
        p += authors_len;
        p++;
        uint16_t doi_len = *((uint16_t *) p);
        p += 2;
        uint8_t *doi = p;

//        printf("%d %d %d\n", title_len, authors_len, doi_len);
//        printf("%s %s %s\n", title, authors, doi);

        title_metrics_t tm;

//        ret = match_title(output_utext, output_utext_len, title-1, title_len +1, &tm);
//
//        if (!ret) continue;

        ret = match_authors(output_text, authors - 1, authors_len + 1);

        if (!ret) continue;

        memcpy(metadata.identifiers[metadata.identifiers_len], "doi:", 4);
        memcpy(metadata.identifiers[metadata.identifiers_len++] + 4, doi, doi_len);

        result->metadata = metadata;
        return 0;
    }

    while (db_get_next_field(stmt, &data, &data_len)) {
        if (data[0] == 1) {
            title_metrics_t tm;
            ret = match_title(output_utext, output_utext_len, data, data_len, &tm);

            if (ret) {
                if (compare_titles(&tm, &best_tm)) {
                    memcpy(metadata.title, data + 1, data_len - 1);
                    metadata.title[data_len - 1] = 0;
                }
            }
        } else if (data[0] == 2) {
            ret = match_authors(output_text, data, data_len);

            if (ret && strlen(metadata.authors) < data_len - 1) {
                memcpy(metadata.authors, data + 1, data_len - 1);
                metadata.authors[data_len - 1] = 0;
            }
        } else if (data[0] == 3) {
//            uint8_t abstract[sizeof(metadata.abstract)];
//            uint32_t abstract_len;
//            ret = extract_abstract(output_text, output_text_len,
//                                       text, 0,
//                                       map, map_len,
//                                       data, data_len, abstract, &abstract_len);
//            if (ret && strlen(metadata.abstract) < strlen(abstract)) {
//                strcpy(metadata.abstract, abstract);
//            }
        } else if (data[0] == 4) {
            uint16_t year = *((uint16_t *) (data + 1));
            int32_t offset = get_year_offset(text, strlen(text), year);
            if (offset >= metadata.year_offset) {
                metadata.year = year;
                metadata.year_offset = offset;
            }
        } else if (data[0] == 5) {
            if (metadata.identifiers_len < MAX_IDENTIFIERS) {
                memcpy(metadata.identifiers[metadata.identifiers_len++], data + 1, data_len - 1);
            }
        }
    }

    if (strlen(metadata.title) && strlen(metadata.authors)) {
        if (strlen(metadata.title) > strlen(result->metadata.title)) {
            result->metadata = metadata;
        }
    }
}

// Todo: Don't process the same title multiple times
uint32_t recognize(uint8_t *file_hash_str, uint8_t *text, result_t *result) {
    memset(result, 0, sizeof(result_t));
//
//    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
//    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
//
//    uint32_t map[MAX_LOOKUP_TEXT_LEN];
//    uint32_t map_len = MAX_LOOKUP_TEXT_LEN;
//
//    line_t lines[MAX_LOOKUP_TEXT_LEN];
//    uint32_t lines_len = MAX_LOOKUP_TEXT_LEN;
//
//    page_t pages[MAX_LOOKUP_TEXT_LEN];
//    uint32_t pages_len = MAX_LOOKUP_TEXT_LEN;
//
//    text_process(text, output_text, &output_text_len, map, &map_len, lines, &lines_len, pages, &pages_len);
//
//    uint32_t output_utext[MAX_LOOKUP_TEXT_LEN];
//    uint32_t output_utext_len;
//
//    text_process2(text, output_utext, &output_utext_len, MAX_LOOKUP_TEXT_LEN - 1);
//
//
//    uint64_t title_hash = 0;
//    if (file_hash_str && strlen(file_hash_str) == 32) {
//        uint8_t buf[17];
//        memcpy(buf, file_hash_str, 16);
//        buf[16] = 0;
//        uint64_t file_hash = strtoul(buf, 0, 16);
//
//        uint64_t mhs[100];
//        uint32_t mhs_len = 100;
//        db_fhmhs(file_hash, mhs, &mhs_len);
//
//        if (mhs_len <= MAX_MH) {
//            for (uint32_t i = 0; i < mhs_len; i++) {
//                result->detected_metadata_through_hash++;
//                process_metadata(result, text, mhs[i], output_text, output_text_len, map, map_len, output_utext,
//                                 output_utext_len);
//            }
//        }
//    }
//
//    if (output_text_len >= HASHABLE_ABSTRACT_LEN) {
//        for (uint32_t i = 0; i < output_text_len - HASHABLE_ABSTRACT_LEN; i++) {
//            uint64_t abstract_hash = text_hash64(output_text + i, HASHABLE_ABSTRACT_LEN);
//            if (ht_get_slot(1, abstract_hash)) {
//                result->detected_abstracts++;
//
//                uint64_t mhs[100];
//                uint32_t mhs_len = 100;
//                db_ahmhs(abstract_hash, mhs, &mhs_len);
//
//                if (mhs_len <= MAX_MH) {
//                    for (uint32_t j = 0; j < mhs_len; j++) {
//                        result->detected_metadata_through_abstract++;
//                        process_metadata(result, text, mhs[j], output_text, output_text_len, map, map_len, output_utext,
//                                         output_utext_len);
//                    }
//                }
//            }
//        }
//    }
//
//    uint32_t tried = 0;
//    for (uint32_t i = 0; i < lines_len && lines[i].start<=3000 && tried <= 1000; i++) {
//        for (uint32_t j = i; j < i + 6 && j < lines_len; j++) {
//
//            uint32_t title_start = lines[i].start;
//            uint32_t title_end = lines[j].end;
//            uint32_t title_len = title_end - title_start + 1;
//
//            if (title_len <= 20 || title_len > 500) continue;
//
//            if(title_len<30 && title_start>500) continue;
//
//            tried++;
//            title_hash = text_hash64(output_text + title_start, title_end - title_start + 1);
//            //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);
//
//            if (ht_get_slot(2, title_hash)) {
//
//                uint8_t blacklisted=0;
//                for(uint32_t k=title_len;k>=15;k--) {
//                    for(uint32_t m=0;m<=title_len-k;m++) {
//                        uint64_t h = text_hash64(output_text + title_start + m, k);
//
//                        if(blacklist_has(h)) {
//                            //printf("black list hash: %lu %.*s\n", h, k, output_text + title_start + m);
//                            blacklisted = 1;
//                            break;
//                        }
//                    }
//                    if(blacklisted) break;
//                }
//
//                if(!blacklisted) {
//                    result->detected_titles++;
//
//                    uint64_t mhs[100];
//                    uint32_t mhs_len = 100;
//                    db_thmhs(title_hash, mhs, &mhs_len);
//
//                    if (mhs_len <= MAX_MH) {
//                        for (uint32_t k = 0; k < mhs_len; k++) {
//                            result->detected_metadata_through_title++;
//                            process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len,
//                                             output_utext,
//                                             output_utext_len);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    for(uint32_t i=0;i<200;i++) {
//        for(uint32_t j=40;j<140;j++) {
//            title_hash = text_hash64(output_text + i, j);
//
//            if (ht_get_slot(2, title_hash)) {
//
//                uint8_t blacklisted=0;
//                for(uint32_t k=j;k>=15;k--) {
//                    for(uint32_t m=0;m<=j-k;m++) {
//                        uint64_t h = text_hash64(output_text + i + m, k);
//
//                        if(blacklist_has(h)) {
//                            blacklisted = 1;
//                            break;
//                        }
//                    }
//                    if(blacklisted) break;
//                }
//
//                if(!blacklisted) {
//                    result->detected_titles++;
//
//                    uint64_t mhs[100];
//                    uint32_t mhs_len = 100;
//                    db_thmhs(title_hash, mhs, &mhs_len);
//
//                    if (mhs_len <= MAX_MH) {
//                        for (uint32_t k = 0; k < mhs_len; k++) {
//                            result->detected_metadata_through_title++;
//                            process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len,
//                                             output_utext,
//                                             output_utext_len);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//
//
//    if(pages_len>=2) {
//        uint8_t *pb = output_text+pages[1].start;
//        for (uint32_t i = 0; i < 200; i++) {
//            for (uint32_t j = 40; j < 140; j++) {
//                title_hash = text_hash64(pb + i, j);
//
//                if (ht_get_slot(2, title_hash)) {
//                    uint8_t blacklisted=0;
//                    for(uint32_t k=j;k>=15;k--) {
//                        for(uint32_t m=0;m<=j-k;m++) {
//                            uint64_t h = text_hash64(output_text + i + m, k);
//
//                            if(blacklist_has(h)) {
//                                blacklisted = 1;
//                                break;
//                            }
//                        }
//                        if(blacklisted) break;
//                    }
//
//                    if(!blacklisted) {
//                        result->detected_titles++;
//
//                        uint64_t mhs[100];
//                        uint32_t mhs_len = 100;
//                        db_thmhs(title_hash, mhs, &mhs_len);
//
//                        if (mhs_len <= MAX_MH) {
//                            for (uint32_t k = 0; k < mhs_len; k++) {
//                                result->detected_metadata_through_title++;
//                                process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len,
//                                                 output_utext,
//                                                 output_utext_len);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }


    if (result->metadata.metadata_hash) return 1;
    return 0;
}

typedef struct group {
    uint8_t *text;
} group_t;


typedef struct word {
    uint8_t space_after;
    double font_size;
    uint8_t *text;
    uint32_t text_len;
    double xMin;
    double xMax;
    double yMin;
    double yMax;
} word_t;

typedef struct line {
    word_t *words;
    uint32_t words_len;
} line_t;

typedef struct block {
    uint8_t alignment;
    uint32_t font_size;
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

doc_t *print_element_names(json_t *body) {

    doc_t *doc = (doc_t *) malloc(sizeof(doc_t));

    json_t *json_pages = json_object_get(body, "pages");
    if (!json_is_array(json_pages)) return 0;
    uint32_t pages_len = json_array_size(json_pages);

    doc->pages = (page_t *) malloc(sizeof(page_t) * pages_len);
    doc->pages_len = pages_len;

    for (uint32_t page_i = 0; page_i < pages_len; page_i++) {
        json_t *json_obj = json_array_get(json_pages, page_i);
        page_t *page = doc->pages + page_i;

        json_t *width = json_object_get(json_obj, "width");
        json_t *height = json_object_get(json_obj, "height");

        page->width = strtod(json_string_value(width), 0);
        page->height = strtod(json_string_value(height), 0);

        json_t *json_flows = json_object_get(json_obj, "flows");
        if (!json_is_array(json_flows)) return 0;
        page->flows_len = json_array_size(json_flows);
        page->flows = (flow_t *) malloc(sizeof(flow_t) * page->flows_len);

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            json_t *json_obj = json_array_get(json_flows, flow_i);
            flow_t *flow = page->flows + flow_i;

            json_t *json_blocks = json_object_get(json_obj, "blocks");
            if (!json_is_array(json_blocks)) return 0;
            flow->blocks_len = json_array_size(json_blocks);
            flow->blocks = (block_t *) calloc(sizeof(block_t), flow->blocks_len);
            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                json_t *json_obj = json_array_get(json_blocks, block_i);
                block_t *block = flow->blocks + block_i;

                block->font_size_min = 0;
                block->font_size_max = 0;

                block->text_len = 0;


                json_t *xMin = json_object_get(json_obj, "xMin");
                json_t *xMax = json_object_get(json_obj, "xMax");
                json_t *yMin = json_object_get(json_obj, "yMin");
                json_t *yMax = json_object_get(json_obj, "yMax");

                block->xMin = strtod(json_string_value(xMin), 0);
                block->xMax = strtod(json_string_value(xMax), 0);
                block->yMin = strtod(json_string_value(yMin), 0);
                block->yMax = strtod(json_string_value(yMax), 0);

                json_t *json_lines = json_object_get(json_obj, "lines");
                if (!json_is_array(json_lines)) return 0;
                block->lines_len = json_array_size(json_lines);
                block->lines = (line_t *) malloc(sizeof(line_t) * block->lines_len);
                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    json_t *json_obj = json_array_get(json_lines, line_i);
                    line_t *line = block->lines + line_i;

                    json_t *json_words = json_object_get(json_obj, "words");
                    if (!json_is_array(json_words)) return 0;
                    line->words_len = json_array_size(json_words);
                    line->words = (word_t *) malloc(sizeof(word_t) * line->words_len);
                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        json_t *json_obj = json_array_get(json_words, word_i);
                        word_t *word = line->words + word_i;

                        json_t *xMin = json_object_get(json_obj, "xMin");
                        json_t *xMax = json_object_get(json_obj, "xMax");
                        json_t *yMin = json_object_get(json_obj, "yMin");
                        json_t *yMax = json_object_get(json_obj, "yMax");
                        json_t *spaceAfter = json_object_get(json_obj, "spaceAfter");
                        json_t *fontSize = json_object_get(json_obj, "fontSize");

                        word->xMin = strtod(json_string_value(xMin), 0);
                        word->xMax = strtod(json_string_value(xMax), 0);
                        word->yMin = strtod(json_string_value(yMin), 0);
                        word->yMax = strtod(json_string_value(yMax), 0);
                        word->font_size = strtod(json_string_value(fontSize), 0);
                        word->space_after = strtol(json_string_value(spaceAfter), 0, 10);


                        if (block->font_size_min == 0 || block->font_size_min < word->font_size) {
                            block->font_size_min = word->font_size;
                        }

                        if (block->font_size_max < word->font_size) {
                            block->font_size_max = word->font_size;
                        }

                        word->text = json_string_value(json_object_get(json_obj, "text"));

                        word->text_len = strlen(word->text);
                        block->text_len += word->text_len;
                    }
                }
            }
        }
    }
    return doc;
}

void print_doc(doc_t *doc) {
    for (uint32_t page_i = 0; page_i < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;
        printf("PAGE: %f %f\n", page->width, page->height);

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;
            printf(" FLOW:\n");

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;
                printf("  BLOCK: %f %f %f %f %f %f %u\n", block->xMin, block->xMax, block->yMin, block->yMax,
                       block->font_size_min, block->font_size_max, block->text_len);

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;
                    printf("   LINE:\n");

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;
                        printf("    WORD: %f %f %f %f %s %f %d\n", word->xMin, word->xMax, word->yMin, word->yMax,
                               word->text, word->font_size, word->space_after);

                    }
                }
            }
        }
    }
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
    double paddingTop;
    double paddingBottom;
    double paddingLeft;
    double paddingRight;
    page_t *page;
    double sq;
    uint8_t upper;
} grouped_blocks_t;

int calculate_paddings(grouped_blocks_t *grouped_blocks, uint32_t grouped_blocks_len) {
    for (uint32_t i = 0; i < grouped_blocks_len; i++) {

        grouped_blocks_t *gb1 = &grouped_blocks[i];

        uint8_t hasPaddingLeft = 0;
        uint8_t hasPaddingRight = 0;
        uint8_t hasPaddingTop = 0;
        uint8_t hasPaddingBottom = 0;

        gb1->paddingLeft = 0;
        gb1->paddingTop = 0;
        gb1->paddingRight = gb1->page->width - 72 - gb1->xMax;
        gb1->paddingBottom = gb1->page->height - 70 - gb1->yMax;

        for (uint32_t j = 0; j < grouped_blocks_len; j++) {
            if (i == j) continue;

            grouped_blocks_t *gb2 = &grouped_blocks[j];

            if (gb1->page != gb2->page) continue;

            uint8_t left = gb2->xMax < gb1->xMin;
            uint8_t right = gb1->xMax < gb2->xMin;
            uint8_t top = gb2->yMax < gb1->yMin;
            uint8_t bottom = gb1->yMax < gb2->yMin;

            double paddingLeft = 0;
            double paddingRight = 0;
            double paddingTop = 0;
            double paddingBottom = 0;

            if (left && top) {
                paddingLeft = gb1->xMin - gb2->xMax;
                paddingTop = gb1->yMin - gb2->yMax;
                if (!hasPaddingLeft || gb1->paddingRight > paddingLeft) gb1->paddingLeft = paddingLeft;
                if (!hasPaddingTop || gb1->paddingTop > paddingTop) gb1->paddingTop = paddingTop;
                hasPaddingLeft = 1;
                hasPaddingTop = 1;
            } else if (left && bottom) {
                paddingLeft = gb1->xMin - gb2->xMax;
                paddingBottom = gb2->yMin - gb1->yMax;
                if (!hasPaddingLeft || gb1->paddingLeft > paddingLeft) gb1->paddingLeft = paddingLeft;
                if (!hasPaddingBottom || gb1->paddingBottom > paddingBottom) gb1->paddingBottom = paddingBottom;
                hasPaddingLeft = 1;
                hasPaddingBottom = 1;
            } else if (right && top) {
                paddingRight = gb2->xMin - gb1->xMax;
                paddingTop = gb1->yMin - gb2->yMax;
                if (!hasPaddingRight || gb1->paddingRight > paddingRight) gb1->paddingRight = paddingRight;
                if (!hasPaddingTop || gb1->paddingTop > paddingTop) gb1->paddingTop = paddingTop;
                hasPaddingRight = 1;
                hasPaddingTop = 1;
            } else if (right && bottom) {
                paddingRight = gb2->xMin - gb1->xMax;
                paddingBottom = gb2->yMin - gb1->yMax;
                if (!hasPaddingRight || gb1->paddingRight > paddingRight) gb1->paddingRight = paddingRight;
                if (!hasPaddingBottom || gb1->paddingBottom > paddingBottom) gb1->paddingBottom = paddingBottom;
                hasPaddingRight = 1;
                hasPaddingBottom = 1;
            } else if (top) {
                paddingTop = gb1->yMin - gb2->yMax;
                if (!hasPaddingTop || gb1->paddingTop > paddingTop) gb1->paddingTop = paddingTop;
                hasPaddingTop = 1;
            } else if (bottom) {
                paddingBottom = gb2->yMin - gb1->yMax;
                if (!hasPaddingBottom || gb1->paddingBottom > paddingBottom) gb1->paddingBottom = paddingBottom;
                hasPaddingBottom = 1;
            } else if (left) {
                paddingLeft = gb1->xMin - gb2->xMax;
                if (!hasPaddingLeft || gb1->paddingLeft > paddingLeft) gb1->paddingLeft = paddingLeft;
                hasPaddingLeft = 1;
            } else if (right) {
                paddingRight = gb2->xMin - gb1->xMax;
                if (!hasPaddingRight || gb1->paddingRight > paddingRight) gb1->paddingRight = paddingRight;
                hasPaddingRight = 1;
            } else {
                gb1->paddingLeft = 0;
                gb1->paddingRight = 0;
                gb1->paddingTop = 0;
                gb1->paddingBottom = 0;
                hasPaddingLeft = 1;
                hasPaddingTop = 1;
                hasPaddingRight = 1;
                hasPaddingBottom = 1;
            }

        }


        gb1->sq = (((gb1->paddingLeft + (gb1->xMax - gb1->xMin) + gb1->paddingRight)) *
                   (gb1->paddingTop + (gb1->yMax - gb1->yMin) + gb1->paddingBottom)) -
                  (gb1->xMax - gb1->xMin) * (gb1->yMax - gb1->yMin);


    }
}


int get_groups(doc_t *doc, grouped_blocks_t *grouped_blocks, uint32_t *grouped_blocks_len) {

    grouped_blocks_t gb_max;
    double gb_max_font_size = 0;

    uint32_t first_page = 0;
    if (doc->pages_len >= 3) {
        if (
                doc->pages[1].width == doc->pages[2].width &&
                doc->pages[1].height == doc->pages[2].height && (
                        doc->pages[0].width != doc->pages[1].width || doc->pages[0].height != doc->pages[1].height)) {
            first_page = 1;
        }
    }

    for (uint32_t page_i = first_page; page_i < doc->pages_len && page_i < 2; page_i++) {
        page_t *page = doc->pages + page_i;


        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;
                if (block->used) continue;


                double max_font_size = block->font_size_max;

                grouped_blocks_t gb;
                memset(&gb, 0, sizeof(grouped_blocks_t));
                gb.page = page;

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
                        if (flow2_i == flow_i && block2_i == block_i) continue;
                        if (
                                (block2->font_size_min >= block->font_size_min &&
                                 block2->font_size_min <= block->font_size_max ||
                                 block2->font_size_max <= block->font_size_max &&
                                 block2->font_size_max >= block->font_size_min)) {

                            if (
                                    gb.blocks[gb.blocks_len - 1]->yMin - block2->yMax > block->font_size_max * 2 ||
                                    block2->yMin - gb.blocks[gb.blocks_len - 1]->yMax > block->font_size_max * 2) {
                                continue;
                            }

                            if (gb.xMin - block2->xMax > 20 || block2->xMax - gb.xMax > 20) continue;


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

//                            printf("B1:\n");
//                            print_block(block);
//                            printf("\nB2:\n");
//                            print_block(block2);
//                            printf("\n\n\n");

                        }
                    }
                }

//max_font_size>gb_max_font_size &&
//                printf("LLL: %f %u\n", max_font_size, gb.text_len);
                //if(gb.text_len<500) {
                gb.max_font_size = max_font_size;
                grouped_blocks[*grouped_blocks_len] = gb;
                (*grouped_blocks_len)++;
                gb_max = gb;

                //gb_max_font_size = max_font_size;
                //}





            }
        }


    }

//    for(uint32_t k=0;k<gb_max.blocks_len;k++) {
//        printf("\nB%d:\n",k);
//        print_block(gb_max.blocks[k]);
//    }
}

int is_block_upper(block_t *block) {
    //
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
//
//int get_groups_upper(doc_t *doc, grouped_blocks_t *grouped_blocks, uint32_t *grouped_blocks_len) {
//
//    grouped_blocks_t gb_max;
//
//    uint32_t first_page = 0;
//    if(doc->pages_len >= 3) {
//        if(
//                doc->pages[1].width == doc->pages[2].width &&
//                doc->pages[1].height == doc->pages[2].height && (
//                        doc->pages[0].width != doc->pages[1].width || doc->pages[0].height != doc->pages[1].height )) {
//            first_page = 1;
//        }
//    }
//
//    for(uint32_t page_i=first_page;page_i<doc->pages_len && page_i<2;page_i++) {
//        page_t *page = doc->pages + page_i;
//
//
//        for(uint32_t flow_i=0;flow_i<page->flows_len;flow_i++) {
//            flow_t *flow = page->flows + flow_i;
//
//            for(uint32_t block_i=0;block_i<flow->blocks_len;block_i++) {
//                block_t *block = flow->blocks + block_i;
//                if(block->used) continue;
//
//                double max_font_size = block->font_size_max;
//
//                grouped_blocks_t gb;
//                memset(&gb, 0, sizeof(grouped_blocks_t));
//                gb.page = page;
//
//                gb.xMin = block->xMin;
//                gb.xMax = block->xMax;
//                gb.yMin = block->yMin;
//                gb.yMax = block->yMax;
//
//                gb.blocks[gb.blocks_len]=block;
//                gb.text_len += block->text_len;
//
//                gb.blocks_len++;
//
//
//
//                for(uint32_t flow2_i=0;flow2_i<page->flows_len;flow2_i++) {
//                    flow_t *flow2 = page->flows + flow2_i;
//
//                    for(uint32_t block2_i=0;block2_i<flow2->blocks_len;block2_i++) {
//                        block_t *block2 = flow2->blocks + block2_i;
//                        if(block2->used) continue;
//                        if(flow2_i==flow_i && block2_i==block_i) continue;
//                        if(is_block_upper(block)
//                                (block2->font_size_min>=block->font_size_min && block2->font_size_min<=block->font_size_max ||
//                                 block2->font_size_max<=block->font_size_max && block2->font_size_max>=block->font_size_min) ) {
//
//                            if(
//                                    gb.blocks[gb.blocks_len-1]->yMin-block2->yMax>block->font_size_max*1.5 ||
//                                    block2->yMin-gb.blocks[gb.blocks_len-1]->yMax>block->font_size_max*1.5) {
//                                continue;
//                            }
//
//                            if(gb.xMin-block2->xMax>20 || block2->xMax-gb.xMax>20) continue;
//
//
//                            if(block2->xMin<gb.xMin) gb.xMin = block2->xMin;
//                            if(block2->xMax>gb.xMax) gb.xMax = block2->xMax;
//                            if(block2->yMin<gb.yMin) gb.yMin = block2->yMin;
//                            if(block2->yMax>gb.yMax) gb.yMax = block2->yMax;
//
//                            if(block2->font_size_max>max_font_size) {
//                                max_font_size = block2->font_size_max;
//                            }
//                            gb.blocks[gb.blocks_len]=block2;
//                            gb.text_len += block2->text_len;
//                            gb.blocks_len++;
//
//                            block2->used=1;
//
////                            printf("B1:\n");
////                            print_block(block);
////                            printf("\nB2:\n");
////                            print_block(block2);
////                            printf("\n\n\n");
//
//                        }
//                    }
//                }
//
////max_font_size>gb_max_font_size &&
//                printf("LLL: %f %u\n", max_font_size, gb.text_len);
//                //if(gb.text_len<500) {
//                gb.max_font_size = max_font_size;
//                grouped_blocks[*grouped_blocks_len] = gb;
//                (*grouped_blocks_len)++;
//                gb_max = gb;
//
//                //gb_max_font_size = max_font_size;
//                //}
//
//
//
//
//
//            }
//        }
//
//
//
//    }
//
////    for(uint32_t k=0;k<gb_max.blocks_len;k++) {
////        printf("\nB%d:\n",k);
////        print_block(gb_max.blocks[k]);
////    }
//}




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

            if (word->space_after) {
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

    uint32_t first_page = 0;
    if (doc->pages_len >= 3) {
        if (
                doc->pages[1].width == doc->pages[2].width &&
                doc->pages[1].height == doc->pages[2].height && (
                        doc->pages[0].width != doc->pages[1].width || doc->pages[0].height != doc->pages[1].height)) {
            first_page = 1;
        }
    }

    for (uint32_t page_i = first_page; page_i < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;
//        printf("PAGE: %f %f\n", page->width, page->height);

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;
//            printf(" FLOW:\n");

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;

                    if (line_i != 0) {
                        *(text + *text_len) = ' ';
                        (*text_len)++;
                    }

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        if ((*text_len) + word->text_len >= max_text_size) {
                            *(text + *text_len) = 0;
                            return 1;
                        }
                        memcpy(text + *text_len, word->text, word->text_len);
                        (*text_len) += word->text_len;

                        if (word->space_after) {
                            *(text + *text_len) = ' ';
                            (*text_len)++;
                        }
                    }
                }

            }
        }
    }
    *(text + *text_len) = 0;

    return 1;
}

int compare_int(const void *b, const void *a) {
    if (((grouped_blocks_t *) a)->max_font_size == ((grouped_blocks_t *) b)->max_font_size) return 0;
    return ((grouped_blocks_t *) a)->max_font_size < ((grouped_blocks_t *) b)->max_font_size ? -1 : 1;
}


int calc_upper(grouped_blocks_t *grouped_blocks, uint32_t grouped_blocks_len) {
    for (uint32_t i = 0; i < grouped_blocks_len; i++) {
        grouped_blocks_t *gb = &grouped_blocks[i];
        uint8_t is_upper = 1;
        for (uint32_t k = 0; k < gb->blocks_len; k++) {
            if (!is_block_upper(gb->blocks[k])) {
                is_upper = 0;
                break;
            }
        }

        gb->upper = is_upper;
    }
}

typedef struct font {
    double font_size;
    uint32_t count;
} font_t;

typedef struct fonts_info {
    font_t *fonts;
    uint32_t fonts_len;
} fonts_info_t;

void increment_font(fonts_info_t *fonts_info, double font_size, uint32_t value) {
    for(uint32_t i=0;i<fonts_info->fonts_len;i++) {
        font_t *font = &fonts_info->fonts[i];
        if(fabs(font->font_size - font_size) < 1.0) {
            font->count+=value;
            return;
        }
    }

    fonts_info->fonts_len++;

    if(fonts_info->fonts_len==1) {
        fonts_info->fonts = (font_t *) malloc(sizeof(font_t) * fonts_info->fonts_len);
    } else {
        fonts_info->fonts = (font_t *) realloc(fonts_info->fonts, sizeof(font_t) * fonts_info->fonts_len);
    }

    fonts_info->fonts[fonts_info->fonts_len-1].font_size = font_size;
    fonts_info->fonts[fonts_info->fonts_len-1].count = value;
}

int init_fonts_info(fonts_info_t *fonts_info, doc_t *doc) {

    fonts_info->fonts_len = 0;

    for (uint32_t page_i = 0; page_i < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;

                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        increment_font(fonts_info, word->font_size, word->text_len);
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
        if(!main_font || main_font->count<font->count) {
            main_font = font;
        }
    }
    return main_font;
}

uint32_t recognize2(uint8_t *file_hash_str, json_t *body, result_t *result) {
    memset(result, 0, sizeof(result_t));
    doc_t *doc = print_element_names(body);

    fonts_info_t fonts_info;
    init_fonts_info(&fonts_info, doc);

//    print_fonts_info(&fonts_info);

    font_t *main_font = get_main_font(&fonts_info);

//    printf("Main FONT: %f %u\n", main_font->font_size, main_font->count);

    uint8_t output_text11[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len11 = MAX_LOOKUP_TEXT_LEN;

    doc_to_text(doc, output_text11, &output_text_len11, MAX_LOOKUP_TEXT_LEN - 1);


    uint8_t output_text22[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len22 = MAX_LOOKUP_TEXT_LEN;
    text_process(output_text11, output_text22, &output_text_len22, 0, 0);


    uint32_t output_utext[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_utext_len = 0;

//    text_process2(output_text11, output_utext, &output_utext_len, MAX_LOOKUP_TEXT_LEN - 1);


//printf("%s\n", output_text11);
    //print_doc(doc);

    grouped_blocks_t grouped_blocks[1000];
    uint32_t grouped_blocks_len = 0;
    get_groups(doc, grouped_blocks, &grouped_blocks_len);

    qsort(grouped_blocks, grouped_blocks_len, sizeof(grouped_blocks_t), compare_int);

//    printf("largest: %f\n", grouped_blocks[0].max_font_size);

    calculate_paddings(grouped_blocks, grouped_blocks_len);

    calc_upper(grouped_blocks, grouped_blocks_len);

    grouped_blocks_t *max_gb = &grouped_blocks[0];
    for (uint32_t i = 0; i < grouped_blocks_len; i++) {
        grouped_blocks_t *gb1 = &grouped_blocks[i];
        if (gb1->sq > max_gb->sq) max_gb = gb1;
    }

    double title_font_size = 0;
    uint8_t title[1024];

    for (uint32_t i = 0; i < grouped_blocks_len; i++) {
        grouped_blocks_t *gb = &grouped_blocks[i];
        //if (gb->sq <= 0) continue;
        //if(main_font->font_size+1.1>gb->max_font_size) continue;
//        printf("%f %f %f %f\n", gb->xMin, gb->xMax, gb->yMin, gb->yMax);
//        printf("%f %f %f %f %f upp:%d\n", gb->paddingLeft, gb->paddingRight, gb->paddingTop, gb->paddingBottom, gb->sq,
//               gb->upper);
//        if (gb == max_gb) printf("MAX FOUND");

        uint8_t text[1024]={0};
        uint32_t text_len=0;
        for (uint32_t k = 0; k < gb->blocks_len; k++) {
//            printf("\nB%d:\n", k);
            if(text_len>=500) break;
            uint32_t block_text_len;
            block_to_text(gb->blocks[k], text+text_len, &block_text_len, 500-text_len);
            text_len+=block_text_len;
        }

        if(text_len<6 || text_len>300) continue;

        if(text_len>30 && title_font_size<gb->max_font_size) {
            strcpy(title, text);
            title_font_size = gb->max_font_size;
        }

//        printf("TXT: %s\n", text);

        uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
        uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
        text_process(text, output_text, &output_text_len, 0, 0);

//        printf("%s\n%s\n\n\n", text, output_text);

        uint64_t title_hash = text_hash64(output_text, output_text_len);
        //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);

        if (ht_get_slot(2, title_hash)) {

//            printf("detected!!!\n");
            result->detected_titles++;

            uint64_t mhs[100];
            uint32_t mhs_len = 100;
            db_thmhs(title_hash, mhs, &mhs_len);

            if (mhs_len <= MAX_MH) {
                for (uint32_t k = 0; k < mhs_len; k++) {
                    result->detected_metadata_through_title++;
                    process_metadata(result, text, mhs[k], output_text22, output_text_len22, 0, 0,
                                     output_utext,
                                     output_utext_len);
                }
            }

        }
    }



    if (result->metadata.metadata_hash) return 1;

    if(title_font_size>1) {
//        printf("THE TITLE:\n%s\n", title);
        strcpy(result->metadata.title, title);
        return 1;
    }

    return 0;
}

