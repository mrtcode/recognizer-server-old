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
#include <time.h>
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"

time_t updated_t = 0;

uint64_t total_indexed = 0;

time_t index_updated_t() {
    return updated_t;
}

uint64_t index_total_indexed() {
    return total_indexed;
}

uint32_t insert_title(uint64_t title_hash, uint8_t *title) {
    if (!title) return 0;

    uint32_t title_len = strlen(title);

    if (title_len >= MAX_TITLE_LEN) return 0;

    uint8_t data[1 + MAX_TITLE_LEN];

    data[0] = 1;
    memcpy(data + 1, title, title_len);

    db_fields_insert(title_hash, data, 1 + title_len);
    return 1;
}

uint32_t insert_authors(uint64_t title_hash, uint8_t *authors) {
    if (!authors) return 0;

    uint32_t authors_len = strlen(authors);

    if (authors_len >= MAX_AUTHORS_LEN) return 0;

    uint8_t data[1 + MAX_AUTHORS_LEN];
    uint32_t data_len = 0;

    data[0] = 2;
    memcpy(data + 1, authors, authors_len);
    data_len = 1 + authors_len;

    db_fields_insert(title_hash, data, data_len);
    return 1;
}

uint32_t insert_abstract(uint64_t title_hash, uint8_t *abstract) {
    if (!abstract) return 0;

    uint8_t processed_abstract[MAX_ABSTRACT_LEN + 1];
    uint32_t processed_abstract_len = MAX_ABSTRACT_LEN + 1;

    if(!text_process_field(abstract, processed_abstract, &processed_abstract_len, 0)) return 0;

    uint8_t data[11];
    data[0] = 3;
    *((uint16_t *) (data + 1)) = processed_abstract_len;
    *((uint64_t *) (data + 3)) = text_hash64(processed_abstract, processed_abstract_len);

    db_fields_insert(title_hash, data, 11);

    uint64_t abstract_hash = text_hash56(processed_abstract, HASHABLE_ABSTRACT_LEN);

    if (!ht_get_slot(1, abstract_hash)) {
        ht_add_slot(1, abstract_hash);
    }

    db_ahth_insert(abstract_hash, title_hash);

    return 1;
}

uint32_t insert_year(uint64_t title_hash, uint8_t *year) {
    if (!year) return 0;

    uint8_t data[3];

    uint16_t year_number = strtoul(year, 0, 10);

    if (year_number < 1900 || year_number > 2025) {
        return 0;
    }

    data[0] = 4;
    *((uint16_t *) (data + 1)) = year_number;

    db_fields_insert(title_hash, data, 3);

    return 1;
}

// Todo: Validate identifiers
uint32_t insert_identifiers(uint64_t title_hash, uint8_t *identifiers) {
    if (!identifiers) return 0;

    uint8_t data[1 + MAX_IDENTIFIER_LEN];
    uint32_t data_len = 0;

    uint32_t count = 0;

    uint8_t *p = identifiers;
    uint8_t *s;

    while (*p && count < MAX_IDENTIFIERS_PER_ITEM) {
        while (*p == '\n') p++;
        if (!*p) break;
        s = p;
        while (*p && *p != '\n') p++;

        if (p - s > MAX_IDENTIFIER_LEN) continue;

        data[0] = 5;
        memcpy(data + 1, s, p - s);
        data_len = (uint32_t) (1 + p - s);

        db_fields_insert(title_hash, data, data_len);
        count++;
    }
    return 1;
}

uint32_t insert_hash(uint64_t title_hash, uint8_t *hash) {
    if (!hash || strlen(hash) != 32) return 0;
    uint8_t buf[17];
    strncpy(buf, hash, 16);
    uint64_t file_hash = strtoul(buf, 0, 16);
    db_fhth_insert(file_hash, title_hash);
    return 1;
}

uint32_t index_metadata(metadata_t *metadata) {
    uint8_t processed_title[MAX_TITLE_LEN + 1];
    uint32_t processed_title_len = MAX_TITLE_LEN + 1;

    if (!metadata->title) return 0;

    text_process(metadata->title, processed_title, &processed_title_len, 0, 0, 0, 0);

    if (processed_title_len < 5) return 0;

    uint64_t title_hash = text_hash56(processed_title, processed_title_len);
    //printf("Index: %lu %.*s\n", title_hash, output_text_len, output_text);

    if (!ht_get_slot(2, title_hash)) {
        ht_add_slot(2, title_hash);
    }

    insert_title(title_hash, metadata->title);
    insert_authors(title_hash, metadata->authors);
    insert_abstract(title_hash, metadata->abstract);
    insert_year(title_hash, metadata->year);
    insert_identifiers(title_hash, metadata->identifiers);
    insert_hash(title_hash, metadata->hash);

    total_indexed++;
    if (total_indexed % 10000 == 0) {
        printf("%u\n", total_indexed);
    }

    updated_t = time(0);

    return 1;
}
