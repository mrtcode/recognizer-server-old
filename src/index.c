/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2018 Zotero
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
#include <pthread.h>
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"

time_t updated_t = 0;

uint64_t total_indexed = 0;

extern pthread_rwlock_t data_rwlock;

time_t index_updated_t() {
    return updated_t;
}

uint64_t index_total_indexed() {
    return total_indexed;
}

uint32_t index_metadata2(uint8_t *title, uint8_t *authors, uint8_t *doi) {
    if (!title) return 0;

    uint8_t processed_title[MAX_TITLE_LEN + 1];
    uint32_t processed_title_len = MAX_TITLE_LEN + 1;

    if(!text_process(title, processed_title, &processed_title_len)) return 0;

    if (processed_title_len < 5) return 0;

    uint64_t title_hash = text_hash64(processed_title, processed_title_len);

    uint8_t *data = authors;
    uint32_t data_len = strlen(authors);
    uint8_t *p = authors;
    uint8_t *s;

    uint8_t *first_name, *last_name;

    uint32_t authors_total = 0;
    uint32_t authors_found = 0;

    uint32_t author_hashes[10]={0};
    uint8_t author_lengths[10]={0};

    while (p - data < data_len - 1) {
        while ((*p == '\t' || *p == '\n')) p++;
        if (!*p) break;
        s = p;
        while ( *p && *p != '\t' && *p != '\n') p++;

        if (*p == '\t') {
            first_name = s;
        } else {
            authors_total++;
            last_name = s;
            uint8_t norm_last_name[64];
            uint32_t norm_last_name_len = 64;
            text_process_fieldn(last_name, p - s, norm_last_name, &norm_last_name_len);

            author_hashes[authors_total-1] = text_hash32(norm_last_name, norm_last_name_len);
            author_lengths[authors_total-1] = (uint8_t)norm_last_name_len;

            if(authors_total>=2) break;
        }
    }

    slot_t *slots[100];
    uint32_t slots_len;

    pthread_rwlock_wrlock(&data_rwlock);

    ht_get_slots(title_hash, slots, &slots_len);
    if (slots_len<5) {
        uint32_t doi_id = ht_add_slot(title_hash,
                    author_lengths[0], author_lengths[1],
                    author_hashes[0], author_hashes[1]
        );

        db_dois_insert(doi_id, doi, strlen(doi));
    }

    pthread_rwlock_unlock(&data_rwlock);

    total_indexed++;
    updated_t = time(0);
}
