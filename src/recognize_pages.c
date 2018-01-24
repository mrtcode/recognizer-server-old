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
#include "recognize_pages.h"

int extract_pages(doc_t *doc, uint32_t *start, uint32_t *first) {
    for (uint32_t page_i = 0; page_i + 2 < doc->pages_len; page_i++) {
        page_t *page = doc->pages + page_i;

        for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
            flow_t *flow = page->flows + flow_i;

            for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
                block_t *block = flow->blocks + block_i;

                for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                    line_t *line = block->lines + line_i;
                    if (line->yMax < 100 || line->yMin > page->height - 100) {

                        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                            word_t *word = line->words + word_i;

                            if(
                                    !(fabs(page->content_x_left-word->xMin)<5.0 ||
                                    fabs(page->content_x_right-word->xMax)<5.0 ||
                                    fabs((page->content_x_right-page->content_x_left)/2-(word->xMin+(word->xMax-word->xMin)/2))<5.0)) continue;

                            page_t *page2 = doc->pages + page_i + 2;

                            for (uint32_t flow2_i = 0; flow2_i < page2->flows_len; flow2_i++) {
                                flow_t *flow2 = page2->flows + flow2_i;

                                for (uint32_t block2_i = 0; block2_i < flow2->blocks_len; block2_i++) {
                                    block_t *block2 = flow2->blocks + block2_i;

                                    for (uint32_t line2_i = 0; line2_i < block2->lines_len; line2_i++) {
                                        line_t *line2 = block2->lines + line2_i;
                                        if (line2->yMax < 100 || line2->yMin > page2->height - 100) {

                                            for (uint32_t word2_i = 0; word2_i < line2->words_len; word2_i++) {
                                                word_t *word2 = line2->words + word2_i;

                                                if (
                                                        fabs(word->yMin - word2->yMin) < 1.0 &&
                                                        fabs(word->xMin - word2->xMin) < 15.0) {
                                                    //log_debug("detected: %s %s\n", word->text, word2->text);

                                                    uint8_t *w1;
                                                    uint8_t *w2;

                                                    uint32_t n;

                                                    n = 0;

                                                    int skip = 0;

                                                    for (int i = 0; i < word->text_len && n < 30; i++) {
                                                        if (word->text[i] < '0' || word->text[i] > '9') {
                                                            skip = 1;
                                                            break;
                                                        }
                                                    }

                                                    n = 0;
                                                    for (int i = 0; i < word2->text_len && n < 30; i++) {
                                                        if (word2->text[i] < '0' || word2->text[i] > '9') {
                                                            skip = 1;
                                                            break;
                                                        }
                                                    }

                                                    if (skip) continue;

                                                    w1 = word->text;
                                                    w2 = word2->text;


                                                    log_debug("%s %s\n", w1, w2);

                                                    uint32_t nr1 = atoi(w1);
                                                    uint32_t nr2 = atoi(w2);
//                                                    log_debug("found numbers: %d %d\n", nr1, nr2);
                                                    if (nr1 > 0 && nr2 == nr1 + 1) {
                                                        log_debug("found numbers: %d %d\n", nr1, nr2);
                                                        *start = page_i;
                                                        *first = nr1;
                                                        return 1;
                                                    }

                                                }


                                            }

                                        }
                                    }

                                }
                            }


                        }

                    }
                }

            }
        }

    }

    return 0;
}
