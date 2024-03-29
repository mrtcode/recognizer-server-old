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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <jemalloc/jemalloc.h>
#include "log.h"
#include "journal.h"

#define ROWS 1048576
#define ROW_SLOTS_MAX 256

typedef struct row {
    uint8_t *slots;
    uint32_t slots_len;
} row_t;

row_t *journal_rows = 0;

uint32_t journal_init(uint8_t *directory) {
    if (!(journal_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("journal_rows calloc error");
        return 0;
    }

    FILE *fp;
    uint64_t file_size;
    uint8_t path[PATH_MAX];

    snprintf(path, PATH_MAX, "%s/journal.dat", directory);

    fp = fopen(path, "rb");

    if (!fp) {
        log_error("%s not found", path);
        return 0;
    }

    fseek(fp, 0, SEEK_END);
    file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    uint64_t hashes_len = file_size / sizeof(uint64_t);
    uint64_t *hashes = malloc(hashes_len * sizeof(uint64_t));
    fread(hashes, hashes_len, sizeof(uint64_t), fp);

    for (uint32_t i = 0; i < hashes_len; i++) {
        uint64_t hash = hashes[i];
        journal_add(hash);
    }

    free(hashes);

    return 1;
}

// 24 + 40 + 40 (24 + 64 + 16)
uint8_t journal_add(uint64_t h) {
    const uint32_t slot_size = 8;
    uint32_t hash20 = (uint32_t) (h >> 44);
    uint64_t hash64 = h;

    row_t *row = journal_rows + hash20;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64) {
                return 0;
            }
        }
    }

    if ((row->slots_len == ROW_SLOTS_MAX)) {
        log_error("reached ROW_SLOTS_MAX limit");
        return 0;
    }

    if (row->slots) {
        if (!(row->slots = realloc(row->slots, slot_size * (row->slots_len + 1)))) {
            log_error("slot realloc failed");
            return 0;
        }
    } else {
        if (!(row->slots = malloc(slot_size))) {
            log_error("slot malloc failed");
            return 0;
        }
    }

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;

    row->slots_len++;

    return 1;
}

uint8_t journal_has(uint64_t h) {
    const uint32_t slot_size = 8;
    uint32_t hash20 = (uint32_t) (h >> 44);
    uint64_t hash64 = h;

    row_t *row = journal_rows + hash20;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64) {
                return 1;
            }
        }
    }
    return 0;
}
