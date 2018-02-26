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
#include "word.h"

#define ROWS 16777216
#define ROW_SLOTS_MAX 16384

uint64_t word_unique_num = 0;
uint64_t word_feeded_num = 0;

typedef struct row {
    uint8_t *slots;
    uint32_t slots_len;
} row_t;

row_t *word_rows = 0;

uint32_t word_init(uint8_t *directory) {
    if (!(word_rows = calloc(ROWS, sizeof(row_t)))) {
        log_error("word_rows calloc error");
        return 0;
    }

    FILE *fp;
    uint64_t file_size;
    uint8_t path[PATH_MAX];

    snprintf(path, PATH_MAX, "%s/word.dat", directory);

    fp = fopen(path, "rb");

    if (!fp) {
        log_error("%s not found", path);
        return 0;
    }

    fseek(fp, 0, SEEK_END);
    file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    uint64_t hashes_len = file_size / 20;
    uint8_t *hashes = malloc(hashes_len * 20);
    fread(hashes, hashes_len, 20, fp);

    for (uint32_t i = 0; i < hashes_len; i++) {
        uint64_t hash = *((uint64_t *) (hashes + (i * 20)));
        uint32_t a = *((uint32_t *) (hashes + (i * 20) + 8 + (4 * 0)));
        uint32_t b = *((uint32_t *) (hashes + (i * 20) + 8 + (4 * 1)));
        uint32_t c = *((uint32_t *) (hashes + (i * 20) + 8 + (4 * 2)));

        word_add(hash, a, b, c);
        //log_debug("%lu %u %u %u\n", hash, a, b, c);
    }

    free(hashes);

    return 1;
}

// 24 + 40 + 40 (24 + 64 + 16)
uint8_t word_add(uint64_t h, uint32_t aa, uint32_t bb, uint32_t cc) {
    word_feeded_num++;

//    if(word_feeded_num%10000==0) {
//        log_debug("%lu %lu\n", word_feeded_num, word_unique_num);
//    }

    const uint32_t slot_size = 20;
    uint32_t hash24 = (uint32_t) (h >> 40);
    uint64_t hash64 = h;

    row_t *row = word_rows + hash24;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64) {
                uint32_t *a = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 0));
                uint32_t *b = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 1));
                uint32_t *c = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 2));

                (*a) += aa;
                (*b) += bb;
                (*c) += cc;

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

    word_unique_num++;

    *((uint64_t *) (row->slots + slot_size * row->slots_len)) = hash64;

    uint32_t *a = (uint32_t *) (row->slots + slot_size * row->slots_len + 8 + (4 * 0));
    uint32_t *b = (uint32_t *) (row->slots + slot_size * row->slots_len + 8 + (4 * 1));
    uint32_t *c = (uint32_t *) (row->slots + slot_size * row->slots_len + 8 + (4 * 2));

    *a = 0;
    *b = 0;
    *c = 0;

    (*a) += aa;
    (*b) += bb;
    (*c) += cc;

    row->slots_len++;

    return 1;
}

uint8_t word_get(uint64_t h, uint32_t *a, uint32_t *b, uint32_t *c) {
    const uint32_t slot_size = 20;
    uint32_t hash24 = (uint32_t) (h >> 40);
    uint64_t hash64 = h;

    row_t *row = word_rows + hash24;

    if (row->slots) {
        for (uint32_t i = 0; i < row->slots_len; i++) {
            if (*((uint64_t *) (row->slots + slot_size * i)) == hash64) {
                uint32_t *a1 = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 0));
                uint32_t *b1 = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 1));
                uint32_t *c1 = (uint32_t *) (row->slots + slot_size * i + 8 + (4 * 2));

                *a = *a1;
                *b = *b1;
                *c = *c1;
                return 1;
            }
        }
    }
    return 0;
}
