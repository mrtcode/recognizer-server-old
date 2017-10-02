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
#include "log.h"

#define ROWS 16777216
#define ROW_SLOTS_MAX 16384

typedef struct row {
    uint8_t *slots;
    uint32_t slots_len;
} row_t;

row_t *fields_rows = 0;
row_t *thmh_rows = 0;
row_t *fhmh_rows = 0;
row_t *ahmh_rows = 0;

int dedup_init() {
    if (!(fields_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("fields_rows calloc error");
        return DEDUP_ERROR;
    }

    if (!(thmh_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("thmh_rows calloc error");
        return DEDUP_ERROR;
    }

    if (!(fhmh_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("fhmh_rows calloc error");
        return DEDUP_ERROR;
    }

    if (!(ahmh_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("ahmh_rows calloc error");
        return DEDUP_ERROR;
    }

    return DEDUP_SUCCESS;
}

// 24 + 40 + 40 (24 + 64 + 16)
uint8_t dedup_fields(uint64_t mh, uint64_t dh) {
    const uint32_t slot_size = 10;
    uint32_t hash24 = (uint32_t) (mh >> 40);
    uint64_t hash64 = mh << 24 | dh >> 16;
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
        log_error("reached ROW_SLOTS_MAX limit");
        return DEDUP_ERROR;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            log_error("slot realloc failed");
            return DEDUP_ERROR;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            log_error("slot malloc failed");
            return DEDUP_ERROR;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;
    *((uint16_t *) (row->slots + slot_size * row->slots_len + 8)) = hash16;

    row->slots_len++;

    return DEDUP_SUCCESS;
}

// 24 + 40 + 64 (24 + 64 + 32 + 8)
uint8_t dedup_hmh(uint8_t type, uint64_t h, uint64_t mh) {
    row_t *row;

    if (type == 1) {
        row = thmh_rows;
    } else if (type == 2) {
        row = ahmh_rows;
    } else if (type == 3) {
        row = fhmh_rows;
    } else {
        return DEDUP_ERROR;
    }

    const uint32_t slot_size = 13;
    uint32_t hash24 = (uint32_t) (h >> 40);
    uint64_t hash64 = h << 24 | mh >> 40;
    uint32_t hash32 = (uint32_t) (mh >> 8);
    uint8_t hash8 = (uint8_t) mh;
    row += hash24;

    uint64_t th40 = h << 24;

    if (row->slots) {
        uint32_t n = 0;
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (((*((uint64_t *) (row->slots + slot_size * i))) & 0xFFFFFFFFFF000000) == th40) {
                n++;
                if (n >= 20) {
                    return DEDUP_DUPLICATED;
                }
            }
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64 &&
                *((uint32_t *) (row->slots + slot_size * i + 8)) == hash32 &&
                *((uint8_t *) (row->slots + slot_size * i + 8 + 4)) == hash8) {
                return DEDUP_DUPLICATED;
            }
        }
    }

    if ((row->slots_len == ROW_SLOTS_MAX)) {
        log_error("reached ROW_SLOTS_MAX limit");
        return DEDUP_ERROR;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            log_error("realloc");
            return DEDUP_ERROR;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            log_error("malloc");
            return DEDUP_ERROR;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;
    *((uint32_t *) (row->slots + slot_size * row->slots_len + 8)) = hash32;
    *((uint8_t *) (row->slots + slot_size * row->slots_len + 8 + 4)) = hash8;

    row->slots_len++;
    return DEDUP_SUCCESS;
}
