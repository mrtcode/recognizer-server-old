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

int match_title(uint8_t *original_text, uint32_t original_text_len,
                uint8_t *data, uint32_t data_len) {
    uint8_t *p = data + 1;
    uint8_t *s;

    uint8_t *b;

    uint32_t tokens_total = 0;
    uint32_t tokens_found = 0;

    while (p - data < data_len - 1) {
        while (p - data < data_len - 1 && (*p == ' ' || *p == '\t' || *p == '\n')) p++;
        if (p - data >= data_len - 1) break;
        s = p;
        while (p - data < data_len - 1 && *p != ' ' && *p != '\t' && *p != '\n') p++;

        tokens_total++;

        b = original_text;
        while (*b) {
            uint8_t *c = s;
            while (c < p && *b) {
                if (*c != *b++) break;
                c++;
            }
            if (c >= p) {
                tokens_found++;
                break;
            }
        }
    }

    if (tokens_found >= tokens_total - 2) return 1;

    return 0;
}

int match_authors(uint8_t *original_text, uint8_t *data, uint32_t data_len) {
    uint8_t *p = data + 1;
    uint8_t *s;

    uint8_t *first_name, *last_name;

    uint8_t *a, *b = original_text;

    while (p - data < data_len - 1) {
        while (p - data < data_len - 1 && (*p == '\t' || *p == '\n')) p++;
        if (p - data >= data_len - 1) break;
        s = p;
        while (p - data < data_len - 1 && *p != '\t' && *p != '\n') p++;

        if (*p == '\t') {
            first_name = s;
            //printf("first: %s\n", s);
        } else {
            last_name = s;
            //printf("last: %s\n", s);
            uint8_t found = 0;
            while (*b) {
                uint8_t *c = last_name;
                while (c < p && *b) {
                    if (*c != *b++) break;
                    c++;
                }
                if (c >= p) {
                    found = 1;
                    break;
                }
            }

            if (!found) return 0;
        }
    }
    return 1;
}

int extract_abstract(uint8_t *text, uint32_t text_len,
                     uint8_t *original_text, uint32_t original_text_len,
                     uint32_t *map, uint32_t map_len,
                     uint8_t *data, uint32_t data_len,
                     uint8_t *output_text, uint32_t *output_text_len) {

    uint8_t *p = text;

    uint16_t len = *((uint16_t *) (data + 1));
    uint64_t hash = *((uint64_t *) (data + 3));

    if (!len) return 0;

    while (p < text + text_len - len) {
        //printf("oa: %ul\n", text_hash64(p, len));

        if (text_hash64(p, len) == hash) {
            //text_raw_abstract(original_text, map, map_len, p - text, p - text + len - 1, output_text, 10000);
            return 1;
        }
        p++;
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

int add_identifier(result_t *result, uint64_t title_hash, uint8_t *identifier, uint32_t identifier_len) {
    if (result->identifiers_len == sizeof(result->identifiers) / sizeof(res_identifier_t)) {
        return 0;
    }

    for (uint32_t i = 0; i < result->identifiers_len; i++) {
        uint8_t *p = result->identifiers[i].str;
        uint8_t *v = identifier;
        while (*p && *v) {
            if (*p != *v) break;
            p++;
            v++;
        }
        if (!*p && !*v) return 0;
    }

    result->identifiers[result->identifiers_len].title_hash = title_hash;
    strcpy(result->identifiers[result->identifiers_len].str, identifier);

    result->identifiers_len++;

    return 1;
}

int get_data(result_t *result, uint8_t *text, uint64_t title_hash,
             uint8_t *output_text, uint32_t output_text_len, uint32_t *map, uint32_t map_len) {
    sqlite3_stmt *stmt = db_get_fields_stmt(title_hash);

    uint8_t *data;
    uint32_t data_len;

    res_metadata_t metadata = {0};

    metadata.title_hash = title_hash;

    while (db_get_next_field(stmt, &data, &data_len)) {
        //printf("field: %d\n", data[0]);

        if (data[0] == 1) {
            int ret = match_title(text, 0, data, data_len);

            if (ret && strlen(metadata.title) < strlen(data + 1)) {
                strcpy(metadata.title, data + 1);
            }
        } else if (data[0] == 2) {
            int ret = match_authors(text, data, data_len);

            if (ret && strlen(metadata.authors) < strlen(data + 1)) {
                strcpy(metadata.authors, data + 1);
            }
        } else if (data[0] == 3) {
            uint8_t abstract[sizeof(metadata.abstract)];
            uint32_t abstract_len;
            int ret = extract_abstract(output_text, output_text_len,
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
            add_identifier(result, title_hash, data + 1, data_len - 1);
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

    char output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;

    uint32_t map[MAX_LOOKUP_TEXT_LEN];
    uint32_t map_len = MAX_LOOKUP_TEXT_LEN;

    line_t lines[MAX_LOOKUP_TEXT_LEN];
    uint32_t lines_len = MAX_LOOKUP_TEXT_LEN;

    text_process(text, output_text, &output_text_len, map, &map_len, lines, &lines_len);

    uint64_t title_hash = 0;
    if (file_hash_str && strlen(file_hash_str) == 32) {
        uint8_t buf[17];
        strncpy(buf, file_hash_str, 16);
        uint64_t file_hash = strtoul(buf, 0, 16);
        sqlite3_stmt *stmt = db_fhth_get_stmt(file_hash);
        while ((title_hash = db_fhth_get_next_th(stmt))) {
            result->detected_titles_through_hash++;
            get_data(result, text, title_hash, output_text, output_text_len, map, map_len);
        }
    }

    if (output_text_len) {
        for (uint32_t i = 0; i < output_text_len - 100; i++) {
            uint64_t abstract_hash = text_hash56(output_text + i, 100);
            if (ht_get_slot(1, abstract_hash)) {
                result->detected_abstracts++;
                sqlite3_stmt *stmt = db_ahth_get_stmt(abstract_hash);
                while ((title_hash = db_ahth_get_next_th(stmt))) {
                    result->detected_titles_through_abstract++;
                    get_data(result, text, title_hash, output_text, output_text_len, map, map_len);
                }
            }
        }
    }

    uint32_t tried = 0;
    for (uint32_t i = 0; i < lines_len && tried <= 1000; i++) {
        for (uint32_t j = i; j < i + 5 && j < lines_len; j++) {

            uint32_t title_start = lines[i].start;
            uint32_t title_end = lines[j].end;
            uint32_t title_len = title_end - title_start + 1;

            // Title ngram must be at least 20 bytes len which results to about two normal length latin words or 5-7 chinese characters
            if (title_len < 20 || title_len > 500) continue;

            tried++;
            title_hash = text_hash56(output_text + title_start, title_end - title_start + 1);
            //printf("Lookup: %ul %.*s\n", hash, title_end-title_start+1, output_text+title_start);

            if (ht_get_slot(2, title_hash)) {
                result->detected_titles++;
                get_data(result, text, title_hash, output_text, output_text_len, map, map_len);
            }
        }
    }

    if (result->metadata.title_hash || strlen(result->identifiers)) return 1;
    return 0;
}
