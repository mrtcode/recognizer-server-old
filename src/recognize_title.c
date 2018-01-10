#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <jansson.h>
#include <math.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <unicode/uregex.h>
#include "defines.h"
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "log.h"
#include "wordlist.h"
#include "journal.h"
#include "recognize_title.h"

int is_block_upper(block_t *block) {
    for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
        line_t *line = block->lines + line_i;

        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
            word_t *word = line->words + word_i;

            text_info_t text_info = text_get_info(word->text);
            if (text_info.lowercase) return 0;
        }
    }
    return 1;
}

int compare_title_blocks(const void *b, const void *a) {
    if (((title_blocks_t *) a)->max_font_size == ((title_blocks_t *) b)->max_font_size) return 0;
    return ((title_blocks_t *) a)->max_font_size < ((title_blocks_t *) b)->max_font_size ? -1 : 1;
}

int get_title_blocks(page_t *page, title_blocks_t *title_blocks, uint32_t *title_blocks_len, uint32_t title_blocks_size) {

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {

        if(*title_blocks_len==title_blocks_size) break;


        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {

            if(*title_blocks_len==title_blocks_size) break;

            block_t *block = flow->blocks + block_i;
            if (block->used) continue;

            if (block->yMin <= 50) continue;

            if (block->lines[0].words[0].rotation != 0) continue;

            double max_font_size = block->font_size_max;

            title_blocks_t gb;
            memset(&gb, 0, sizeof(title_blocks_t));

            gb.upper = is_block_upper(block);

            gb.xMin = block->xMin;
            gb.xMax = block->xMax;
            gb.yMin = block->yMin;
            gb.yMax = block->yMax;

            gb.blocks[gb.blocks_len] = block;
            gb.text_len += block->text_len;

            gb.blocks_len++;

            for (uint32_t flow2_i = 0; flow2_i < page->flows_len; flow2_i++) {
                flow_t *flow2 = page->flows + flow2_i;

                for (uint32_t block2_i = 0; block2_i < flow2->blocks_len; block2_i++) {
                    block_t *block2 = flow2->blocks + block2_i;

                    if (block2->used) continue;
                    if (block2->yMin <= 50) continue;
                    if (flow2_i == flow_i && block2_i == block_i) continue;
                    if ((!is_block_upper(block) && !is_block_upper(block2) ||
                         is_block_upper(block) && is_block_upper(block2)) &&
                        fabs(block->lines[0].words[0].font_size - block2->lines[0].words[0].font_size) < 0.5) {

                        if (
                                gb.blocks[gb.blocks_len - 1]->yMin - block2->yMax > block->font_size_max * 2 ||
                                block2->yMin - gb.blocks[gb.blocks_len - 1]->yMax > block->font_size_max * 2 ||
                                block->lines[0].words[0].bold != block2->lines[0].words[0].bold) {
                            continue;
                        }

//                        if (gb.xMin - block2->xMax > 20 || block2->xMax - gb.xMax > 20) continue;


                        if (!(block->xMin + 10 >= block2->xMin && block->xMax - 10 <= block2->xMax ||
                              block2->xMin + 10 >= block->xMin && block2->xMax - 10 <= block->xMax)) {
                            continue;
                        }


                        if (block2->xMin < gb.xMin) gb.xMin = block2->xMin;
                        if (block2->xMax > gb.xMax) gb.xMax = block2->xMax;
                        if (block2->yMin < gb.yMin) gb.yMin = block2->yMin;
                        if (block2->yMax > gb.yMax) gb.yMax = block2->yMax;

                        if (block2->font_size_max > max_font_size) {
                            max_font_size = block2->font_size_max;
                        }
                        gb.blocks[gb.blocks_len] = block2;
                        gb.text_len += block2->text_len;
                        gb.blocks_len++;

                        block2->used = 1;
                    }
                }
            }

//            printf("BL: %s\n", block->lines[0].words[0].text);

            gb.max_font_size = max_font_size;
            title_blocks[*title_blocks_len] = gb;
            (*title_blocks_len)++;
        }
    }

    qsort(title_blocks, *title_blocks_len, sizeof(title_blocks_t), compare_title_blocks);
}

uint8_t find_author(uint8_t *text, uint32_t text_len, uint32_t author_hash, uint32_t author_len) {
    for (uint32_t i = 0; i < text_len - author_len; i++) {
        if (author_hash == text_hash32(text + i, author_len)) {
            return 1;
        }
    }
    return 0;
}

int get_doi_by_title(uint8_t *title, uint8_t *processed_text, uint32_t processed_text_len, uint8_t *doi) {
    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
    text_process(title, output_text, &output_text_len);

    uint64_t title_hash = text_hash64(output_text, output_text_len);
    //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);

    if (output_text_len < 14) return 0;
    slot_t *slots[100];
    uint32_t slots_len;

    ht_get_slots(title_hash, slots, &slots_len);
    if (slots_len == 1) {
        for (uint32_t j = 0; j < slots_len; j++) {
            slot_t *slot = slots[j];
            uint8_t author1_found = find_author(processed_text, processed_text_len, slot->a1_hash, slot->a1_len);
            uint8_t author2_found = find_author(processed_text, processed_text_len, slot->a2_hash, slot->a2_len);
            uint8_t res = db_get_doi(slot->doi_id, doi);
            if (res && (author1_found || author2_found)) {
                log_info("recognized by title: %s (%d, %d) %s\n", title, author1_found, author2_found, doi);
                return 1;
            }
        }
    }
    return 0;
}
