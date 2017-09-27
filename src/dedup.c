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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <jemalloc/jemalloc.h>
#include "dedup.h"

#define ROWS 16777216
#define ROW_SLOTS_MAX 16384

typedef struct row {
    uint8_t *slots;
    uint32_t slots_len;
} row_t;

row_t *fields_rows = 0;
row_t *fhth_rows = 0;
row_t *ahth_rows = 0;

int dedup_init() {
    if (!(fields_rows = calloc(ROWS, sizeof(row_t)))) {
        fprintf(stderr, "fields_rows calloc error\n");
        return DEDUP_ERROR;
    }

    if (!(fhth_rows = calloc(ROWS, sizeof(row_t)))) {
        fprintf(stderr, "fhth_rows calloc error\n");
        return DEDUP_ERROR;
    }

    if (!(ahth_rows = calloc(ROWS, sizeof(row_t)))) {
        fprintf(stderr, "ahth_rows calloc error\n");
        return DEDUP_ERROR;
    }

    return DEDUP_SUCCESS;
}

// 24 + 40 + 40 (24 + 64 + 16)
uint8_t dedup_fields(uint64_t th, uint64_t dh) {
    const uint32_t slot_size = 10;
    uint32_t hash24 = (uint32_t) (th >> 40);
    uint64_t hash64 = th << 24 | dh >> 16;
    uint16_t hash16 = (uint16_t) dh;
    row_t *row = fields_rows + hash24;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64 &&
                *((uint16_t *) (row->slots + slot_size * i + 8)) == hash16) {
                return DEDUP_DUPLICATED;
            }
        }
    }

    if ((row->slots_len == ROW_SLOTS_MAX)) {
        fprintf(stderr, "reached ROW_SLOTS_MAX limit\n");
        return DEDUP_ERROR;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            fprintf(stderr, "slot realloc failed\n");
            return DEDUP_ERROR;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            fprintf(stderr, "slot malloc failed\n");
            return DEDUP_ERROR;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;
    *((uint16_t *) (row->slots + slot_size * row->slots_len + 8)) = hash16;

    row->slots_len++;

    return DEDUP_SUCCESS;
}

// 24 + 40 + 64 (24 + 64 + 32 + 8)
uint8_t dedup_fhth(uint64_t fh, uint64_t th) {
    const uint32_t slot_size = 13;
    uint32_t hash24 = (uint32_t) (fh >> 40);
    uint64_t hash64 = fh << 24 | th >> 40;
    uint32_t hash32 = (uint32_t) (th >> 8);
    uint8_t hash8 = (uint8_t) th;
    row_t *row = fhth_rows + hash24;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64 &&
                *((uint32_t *) (row->slots + slot_size * i + 8)) == hash32 &&
                *((uint8_t *) (row->slots + slot_size * i + 8 + 4)) == hash8) {
                return DEDUP_DUPLICATED;
            }
        }
    }

    if ((row->slots_len == ROW_SLOTS_MAX)) {
        fprintf(stderr, "reached ROW_SLOTS_MAX limit\n");
        return DEDUP_ERROR;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            fprintf(stderr, "slot realloc failed\n");
            return DEDUP_ERROR;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            fprintf(stderr, "slot malloc failed\n");
            return DEDUP_ERROR;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;
    *((uint32_t *) (row->slots + slot_size * row->slots_len + 8)) = hash32;
    *((uint8_t *) (row->slots + slot_size * row->slots_len + 8 + 4)) = hash8;

    row->slots_len++;

    return DEDUP_SUCCESS;
}

// 24 + 40 + 64 (24 + 64 + 32 + 8)
uint8_t dedup_ahth(uint64_t ah, uint64_t th) {
    const uint32_t slot_size = 13;
    uint32_t hash24 = (uint32_t) (ah >> 40);
    uint64_t hash64 = ah << 24 | th >> 40;
    uint32_t hash32 = (uint32_t) (th >> 8);
    uint8_t hash8 = (uint8_t) th;

    row_t *row = ahth_rows + hash24;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64 &&
                *((uint32_t *) (row->slots + slot_size * i + 8)) == hash32 &&
                *((uint8_t *) (row->slots + slot_size * i + 8 + 4)) == hash8) {
                return DEDUP_DUPLICATED;
            }
        }
    }

    if ((row->slots_len == ROW_SLOTS_MAX)) {
        fprintf(stderr, "reached ROW_SLOTS_MAX limit\n");
        return DEDUP_ERROR;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            fprintf(stderr, "slot realloc failed\n");
            return DEDUP_ERROR;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            fprintf(stderr, "slot malloc failed\n");
            return DEDUP_ERROR;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;
    *((uint32_t *) (row->slots + slot_size * row->slots_len + 8)) = hash32;
    *((uint8_t *) (row->slots + slot_size * row->slots_len + 8 + 4)) = hash8;

    row->slots_len++;

    return DEDUP_SUCCESS;
}
