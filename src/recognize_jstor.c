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
#include "recognize_jstor.h"



int extract_jt(uint8_t *text, uint8_t *regText, uint8_t groups[][2048], uint32_t *groups_len) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    if (uregex_find(regEx, 0, &uStatus)) {
        *groups_len = uregex_groupCount(regEx, &uStatus);

        for (uint32_t i = 1; i < *groups_len + 1; i++) {
            int32_t start = uregex_start(regEx, i, &uStatus);
            int32_t end = uregex_end(regEx, i, &uStatus);

            if (end - start < 512) {
                ucnv_fromUChars(conv, groups[i - 1], target_len, uc + start, end - start, &uStatus);
            }
        }
        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int get_jstor_data(page_t *page, uint8_t *text, uint32_t *text_len, uint32_t max_text_size) {

    *text_len = 0;
    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {

        flow_t *flow = &page->flows[flow_i];

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = &flow->blocks[block_i];

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                uint8_t line_str[512] = {0};
                uint32_t line_str_len = 0;

                for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                    word_t *word = line->words + word_i;

                    if (line_str_len + word->text_len > 500) break;

                    memcpy(line_str + line_str_len, word->text, word->text_len);
                    (line_str_len) += word->text_len;

                    if (word->space) {
                        *(line_str + line_str_len) = ' ';
                        (line_str_len)++;
                    }
                }

                if (*text_len + line_str_len > max_text_size - 5) return 0;
                memcpy(text + *text_len, line_str, line_str_len);
                (*text_len) += line_str_len;

                text[(*text_len)++] = '\n';
//                text[++(*text_len)] = 0;

                if (!strncmp(line_str, "Stable URL: http://www.jstor.org/stable/", 40)) {
                    return 1;
                }
            }
            text[(*text_len)++] = '\n';
        }
    }

    return 0;
}

int extract_jstor(page_t *page, res_metadata_t *result) {
    uint8_t authors[2048] = {0};
    uint8_t source[2048] = {0};
    uint8_t published_by[2048] = {0};

    uint8_t text[4096] = {0};
    uint32_t text_len = 0;

    if (!get_jstor_data(page, text, &text_len, sizeof(text))) return 0;

    uint8_t is_book = 0;
    uint8_t *text_start;

    text_start = strstr(text, "Chapter Title: ");

    if (text_start) {
        is_book = 1;
    } else {
        text_start = strstr(text, "\n\n");
        if (text_start) {
            text_start += 2;
        }
    }

    if (!text_start) text_start = text;

    printf("BLOCK TEXT: %s\n", text_start);

    uint8_t groups[5][2048] = {0};
    uint32_t groups_len = 0;

    if (extract_jt(text, "Stable URL: (http:\\/\\/www.\\jstor\\.org\\/stable\\/(\\S+))", groups,
                   &groups_len)) {
        strcpy(result->url, groups[0]);
        sprintf(result->doi, "10.2307/%s", groups[1]);
    } else {
        return 0;
    }

    if (is_book) {
        strcpy(result->type, "book-chapter");
        if (extract_jt(text_start, "Chapter Title: ((?:\\n|.)*)\\nChapter Author\\(s\\): ((?:\\n|.)*)\\n\\n", groups,
                       &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
        } else if (extract_jt(text_start, "Chapter Title: ((?:\\n|.)*)\\n\\n", groups,
                              &groups_len)) {
            strcpy(result->title, groups[0]);
        }

        if (extract_jt(text_start, "Book Title: ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                       &groups_len)) {
            strcpy(result->container, groups[0]);
        }

        if (extract_jt(text_start, "Book Subtitle: ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                       &groups_len)) {
            strcat(result->container, ": ");
            strcat(result->container, groups[0]);
        }

        if (!*authors && extract_jt(text_start, "Book Author\\(s\\): ((?:\\n|.)*?)\\n(Book |Published by: )", groups,
                                    &groups_len)) {
            strcpy(authors, groups[0]);
        }

        if (extract_jt(text_start, "Published by: ((?:\\n|.)*?)\\nStable URL: ", groups,
                       &groups_len)) {
            strcat(published_by, groups[0]);
        }
    } else {
        strcpy(result->type, "journal-article");

        if (extract_jt(text_start, "((?:\\n|.)*)\\nAuthor\\(s\\): (.*)\\nReview by: (.*)\\nSource: (.*)\\n", groups,
                       &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[2]);
            strcpy(source, groups[3]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nAuthor\\(s\\): (.*)\\nSource: (.*)\\n", groups,
                              &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
            strcpy(source, groups[2]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nReview by: (.*)\\nSource: (.*)\\n", groups, &groups_len)) {
            strcpy(result->title, groups[0]);
            strcpy(authors, groups[1]);
            strcpy(source, groups[2]);
        } else if (extract_jt(text_start, "((?:\\n|.)*)\\nSource: (.*)\\n", groups, &groups_len)) {
            strcpy(result->title, groups[0]);;
            strcpy(source, groups[1]);
        }
    }

    if (*authors) {
        uint8_t *s = authors;
        uint8_t *e;
        while (1) {
            e = strstr(s, ", ");
            if (e) {
                *e = 0;
                strcat(result->authors, s);
                strcat(result->authors, "\n");
                s=e+2;
                continue;
            }

            e = strstr(s, " and ");
            if (e) {
                *e = 0;
                strcat(result->authors, s);
                strcat(result->authors, "\n");

                s=e+5;
                continue;
            }

            strcat(result->authors, s);
            strcat(result->authors, "\n");

            break;
        }
    }

    uint8_t *vol;
    uint8_t *no;
    uint8_t *pg;

    vol = strstr(source, ", Vol. ");

    if (vol) {
        uint8_t *c = vol + 7;
        uint8_t *v = result->volume;
        while (*c >= '0' && *c <= '9' && v - result->volume < 10) {
            *v++ = *c++;
        }

        if (vol - source < sizeof(result->container) - 1) {
            memcpy(result->container, source, vol - source);
            result->container[vol - source] = 0;
        }
    }

    no = strstr(source, ", No. ");

    if (no) {
        uint8_t *c = no + 6;
        uint8_t *v = result->issue;
        while (*c >= '0' && *c <= '9' && v - result->issue < 10) {
            *v++ = *c++;
        }

        if (!*result->container) {
            if (no - source < sizeof(result->container) - 1) {
                memcpy(result->container, source, no - source);
                result->container[no - source] = 0;
            }
        }
    }

    uint8_t *c = source;
    int32_t source_len = strlen(source);

    while (c - source < source_len - 4) {
        if (
                c[0] >= '0' && c[0] <= '9' &&
                c[1] >= '0' && c[1] <= '9' &&
                c[2] >= '0' && c[2] <= '9' &&
                c[3] >= '0' && c[3] <= '9' &&
                c[4] == ')') {
            memcpy(result->year, c, 4);
            break;
        }
        c++;
    }

    pg = strstr(source, ", p. ");
    if (pg) {
        pg += 5;
    } else {
        pg = strstr(source, ", pp. ");
        if (pg) {
            pg += 6;
        }
    }

    if (pg) {
        strcpy(result->pages, pg);
    }

    if (*published_by) {
        uint32_t len = strlen(published_by);
        uint8_t *c = published_by + len - 1;

        while (*c <= 64 && c >= published_by) {
            c--;
        }

        if (c > published_by) {
            memcpy(result->publisher, published_by, c - published_by + 1);
        }

        c = published_by + len - 5;
        if (
                c[0] >= '0' && c[0] <= '9' &&
                c[1] >= '0' && c[1] <= '9' &&
                c[2] >= '0' && c[2] <= '9' &&
                c[3] >= '0' && c[3] <= '9' &&
                c[4] == ')') {
            memcpy(result->year, c, 4);
        }
    }

    return 1;
}
