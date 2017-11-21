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
#include "log.h"

uint32_t doi_id = 0;

row_t rows[HASHTABLE_SIZE] = {0};

uint32_t ht_init() {
    log_info("loading hashtable");
    if (!db_ht_load(rows)) {
        return 0;
    }

    doi_id = db_dois_id_last();
    return 1;
}

uint32_t ht_save() {
    return db_ht_save(rows, HASHTABLE_SIZE);
}

stats_t ht_stats() {
    stats_t stats = {0};
    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        if (rows[i].slots) stats.used_rows++;
        stats.total_ah_slots += rows[i].len;
    }

    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        stats.ah_slots_dist[rows[i].len]++;
    }

    return stats;
}

slot_t *ht_get_slots(uint64_t title_hash, slot_t **slots, uint32_t *slots_len) {
    uint32_t title_hash24 = (uint32_t) (title_hash >> 40);
    uint32_t title_hash32 = (uint32_t) (title_hash >> 8);
    uint8_t title_hash8 = (uint8_t) title_hash;
    row_t *row = rows + title_hash24;

    *slots_len = 0;

    for (uint32_t i = 0; i < row->len; i++) {
        slot_t *slot = row->slots + i;
        if (slot->title_hash32 == title_hash32 && slot->title_hash8 == title_hash8) {
            slots[*slots_len] = &row->slots[i];
            (*slots_len)++;
//            if((*slots_len)>1) {
//                (*slots_len)=0;
//                return 0;
//            }
        }
    }

    return 0;
}

uint32_t ht_add_slot(uint64_t title_hash, uint8_t a1_len, uint8_t a2_len, uint32_t a1_hash, uint32_t a2_hash) {
    uint32_t title_hash24 = (uint32_t) (title_hash >> 40);
    uint32_t title_hash32 = (uint32_t) (title_hash >> 8);
    uint8_t title_hash8 = (uint8_t) title_hash;

    row_t *row = rows + title_hash24;

    if (row->len == ROW_SLOTS_MAX) {
        log_error("reached ROW_SLOTS_MAX limit");
        return 0;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, sizeof(slot_t) * (row->len + 1)))) {
            log_error("realloc");
            return 0;
        };
    } else {
        if (!(row->slots = malloc(sizeof(slot_t)))) {
            log_error("malloc");
            return 0;
        }
    }

    row->updated = 1;



    slot_t *slot = row->slots + row->len;
    slot->title_hash32 = title_hash32;
    slot->title_hash8 = title_hash8;

    slot->a1_len = a1_len;
    slot->a2_len = a2_len;
    slot->a1_hash = a1_hash;
    slot->a2_hash = a2_hash;
    slot->doi_id = ++doi_id;

    row->len++;
    return doi_id;
}
