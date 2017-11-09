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
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "log.h"
#include "fuzzysearch.h"

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

    //if (utext_len > 4096) utext_len = 4096;

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
                     uint8_t *output_utext, uint32_t output_utext_len) {
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

        ret = match_title(output_utext, output_utext_len, title-1, title_len +1, &tm);

        if (!ret) continue;

        ret = match_authors(output_text, authors-1, authors_len+1);

        if (!ret) continue;

        memcpy(metadata.identifiers[metadata.identifiers_len], "doi:", 4);
        memcpy(metadata.identifiers[metadata.identifiers_len++]+4, doi, doi_len);

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
            uint8_t abstract[sizeof(metadata.abstract)];
            uint32_t abstract_len;
            ret = extract_abstract(output_text, output_text_len,
                                       text, 0,
                                       map, map_len,
                                       data, data_len, abstract, &abstract_len);
            if (ret && strlen(metadata.abstract) < strlen(abstract)) {
                strcpy(metadata.abstract, abstract);
            }
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

    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;

    uint32_t map[MAX_LOOKUP_TEXT_LEN];
    uint32_t map_len = MAX_LOOKUP_TEXT_LEN;

    line_t lines[MAX_LOOKUP_TEXT_LEN];
    uint32_t lines_len = MAX_LOOKUP_TEXT_LEN;

    page_t pages[MAX_LOOKUP_TEXT_LEN];
    uint32_t pages_len = MAX_LOOKUP_TEXT_LEN;

    text_process(text, output_text, &output_text_len, map, &map_len, lines, &lines_len, pages, &pages_len);

    uint32_t output_utext[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_utext_len;

    text_process2(text, output_utext, &output_utext_len, MAX_LOOKUP_TEXT_LEN - 1);


    uint64_t title_hash = 0;
    if (file_hash_str && strlen(file_hash_str) == 32) {
        uint8_t buf[17];
        memcpy(buf, file_hash_str, 16);
        buf[16] = 0;
        uint64_t file_hash = strtoul(buf, 0, 16);

        uint64_t mhs[100];
        uint32_t mhs_len = 100;
        db_fhmhs(file_hash, mhs, &mhs_len);

        if (mhs_len <= MAX_MH) {
            for (uint32_t i = 0; i < mhs_len; i++) {
                result->detected_metadata_through_hash++;
                process_metadata(result, text, mhs[i], output_text, output_text_len, map, map_len, output_utext,
                                 output_utext_len);
            }
        }
    }

    if (output_text_len >= HASHABLE_ABSTRACT_LEN) {
        for (uint32_t i = 0; i < output_text_len - HASHABLE_ABSTRACT_LEN; i++) {
            uint64_t abstract_hash = text_hash64(output_text + i, HASHABLE_ABSTRACT_LEN);
            if (ht_get_slot(1, abstract_hash)) {
                result->detected_abstracts++;

                uint64_t mhs[100];
                uint32_t mhs_len = 100;
                db_ahmhs(abstract_hash, mhs, &mhs_len);

                if (mhs_len <= MAX_MH) {
                    for (uint32_t j = 0; j < mhs_len; j++) {
                        result->detected_metadata_through_abstract++;
                        process_metadata(result, text, mhs[j], output_text, output_text_len, map, map_len, output_utext,
                                         output_utext_len);
                    }
                }
            }
        }
    }

    uint32_t tried = 0;
    for (uint32_t i = 0; i < lines_len && lines[i].start<=3000 && tried <= 1000; i++) {
        for (uint32_t j = i; j < i + 6 && j < lines_len; j++) {

            uint32_t title_start = lines[i].start;
            uint32_t title_end = lines[j].end;
            uint32_t title_len = title_end - title_start + 1;

            if (title_len <= 20 || title_len > 500) continue;

            if(title_len<30 && title_start>500) continue;

            tried++;
            title_hash = text_hash64(output_text + title_start, title_end - title_start + 1);
            //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);

            if (ht_get_slot(2, title_hash)) {
                result->detected_titles++;

                uint64_t mhs[100];
                uint32_t mhs_len = 100;
                db_thmhs(title_hash, mhs, &mhs_len);

                if (mhs_len <= MAX_MH) {
                    for (uint32_t k = 0; k < mhs_len; k++) {
                        result->detected_metadata_through_title++;
                        process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len, output_utext,
                                         output_utext_len);
                    }
                }
            }
        }
    }

    for(uint32_t i=0;i<200;i++) {
        for(uint32_t j=40;j<140;j++) {
            title_hash = text_hash64(output_text + i, j);

            if (ht_get_slot(2, title_hash)) {
                result->detected_titles++;

                uint64_t mhs[100];
                uint32_t mhs_len = 100;
                db_thmhs(title_hash, mhs, &mhs_len);

                if (mhs_len <= MAX_MH) {
                    for (uint32_t k = 0; k < mhs_len; k++) {
                        result->detected_metadata_through_title++;
                        process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len, output_utext,
                                         output_utext_len);
                    }
                }
            }
        }
    }



    if(pages_len>=2) {
        uint8_t *pb = output_text+pages[1].start;
        for (uint32_t i = 0; i < 200; i++) {
            for (uint32_t j = 40; j < 140; j++) {
                title_hash = text_hash64(pb + i, j);

                if (ht_get_slot(2, title_hash)) {
                    result->detected_titles++;

                    uint64_t mhs[100];
                    uint32_t mhs_len = 100;
                    db_thmhs(title_hash, mhs, &mhs_len);

                    if (mhs_len <= MAX_MH) {
                        for (uint32_t k = 0; k < mhs_len; k++) {
                            result->detected_metadata_through_title++;
                            process_metadata(result, text, mhs[k], output_text, output_text_len, map, map_len,
                                             output_utext,
                                             output_utext_len);
                        }
                    }
                }
            }
        }
    }


    if (result->metadata.metadata_hash) return 1;
    return 0;
}
