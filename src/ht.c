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

row_t rows[HASHTABLE_SIZE] = {0};

uint32_t ht_init() {
    printf("loading hashtable..\n");
    if (!db_ht_load(rows)) {
        return 0;
    }
    return 1;
}

uint32_t ht_save() {
    return db_ht_save(rows, HASHTABLE_SIZE);
}

stats_t ht_stats() {
    stats_t stats = {0};
    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        if (rows[i].slots) stats.used_rows++;
        stats.total_ah_slots += rows[i].ah_len;
        stats.total_th_slots += rows[i].th_len;
        if (stats.max_ah_slots < rows[i].ah_len) stats.max_ah_slots = rows[i].ah_len;
        if (stats.max_th_slots < rows[i].th_len) stats.max_th_slots = rows[i].th_len;
    }

    for (uint32_t i = 0; i < HASHTABLE_SIZE; i++) {
        stats.ah_slots_dist[rows[i].ah_len]++;
        stats.th_slots_dist[rows[i].th_len]++;
    }

    return stats;
}

slot_t *ht_get_slot(uint8_t type, uint64_t hash) {
    uint32_t hash24 = (uint32_t) (hash >> 40);
    uint32_t hash32 = (uint32_t) (hash >> 8);
    uint8_t hash8 = (uint8_t) hash;
    row_t *row = rows + hash24;

    if (type == 1) {
        for (uint32_t i = 0; i < row->ah_len; i++) {
            if (row->slots[i].hash32 == hash32 && row->slots[i].hash8 == hash8) {
                return &row->slots[i];
            }
        }
    } else {
        for (uint32_t i = row->ah_len; i < row->ah_len+row->th_len; i++) {
            if (row->slots[i].hash32 == hash32 && row->slots[i].hash8 == hash8) {
                return &row->slots[i];
            }
        }
    }
    return 0;
}

uint32_t ht_add_slot(uint8_t type, uint64_t hash) {
    uint32_t hash24 = (uint32_t) (hash >> 40);
    uint32_t hash32 = (uint32_t) (hash >> 8);
    uint8_t hash8 = (uint8_t) hash;
    row_t *row = rows + hash24;

    if ((type == 1 && row->ah_len == ROW_SLOTS_MAX) ||
        (type == 2 && row->th_len == ROW_SLOTS_MAX)) {
        fprintf(stderr, "reached ROW_SLOTS_MAX limit for type %d\n", type);
        return 0;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, sizeof(slot_t) * (row->ah_len + row->th_len + 1)))) {
            fprintf(stderr, "slot realloc failed\n");
            return 0;
        };
    } else {
        if (!(row->slots = malloc(sizeof(slot_t)))) {
            fprintf(stderr, "slot malloc failed\n");
            return 0;
        }
    }

    row->updated = 1;

    slot_t *slot;

    uint32_t i = row->ah_len + row->th_len;
    if (type == 1) {
        while (i > row->ah_len) {
            *(row->slots + i) = *(row->slots + i - 1);
            i--;
        }
        row->ah_len++;
    } else {
        row->th_len++;
    }

    slot = row->slots + i;
    slot->hash32 = hash32;
    slot->hash8 = hash8;
    return 1;
}
