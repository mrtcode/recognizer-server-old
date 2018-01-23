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
#include "recognize_various.h"

int extract_doi(uint8_t *text, uint8_t *doi) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "10.\\d{4,9}\\/[-._;()\\/:A-Za-z0-9]+";
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    int max_len = 0;

    while (uregex_findNext(regEx, &uStatus)) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        if (end - start > max_len) {
            ucnv_fromUChars(conv, doi, DOI_LEN, uc + start, end - start, &uStatus);
            max_len = end - start;
        }

        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_isbn(uint8_t *text, uint8_t *isbn) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "(SBN|sbn)[ \\u2014\\u2013\\u2012-]?(10|13)?[: ]*([0-9X][0-9X \\u2014\\u2013\\u2012-]+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    uint8_t tmp[32] = {0};
    uint32_t tmp_i = 0;

    if (isMatch) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        for (uint32_t i = start; i <= end; i++) {
            if (uc[i] >= '0' && uc[i] <= '9' || uc[i] == 'X') {
                tmp[tmp_i++] = uc[i];
                if (tmp_i > 13) break;
            }
        }

        if (tmp_i == 10 || tmp_i == 13) {
            strcpy(isbn, tmp);
        }

        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_arxiv(uint8_t *text, uint8_t *arxiv) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "arXiv:([a-z+-]+\\/[a-zA-Z0-9]+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);
    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        ucnv_fromUChars(conv, arxiv, target_len, uc + start, end - start, &uStatus);
        ret = 1;
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_year(uint8_t *text, uint8_t *year) {
    uint32_t ret = 0;

    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "(^|\\(|\\s)([0-9]{4})(\\)|\\s|$)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    uint8_t tmp[32] = {0};
    uint32_t tmp_i = 0;

    if (isMatch) {
        int32_t start = uregex_start(regEx, 2, &uStatus);
        int32_t end = uregex_end(regEx, 2, &uStatus);

        uint8_t year_str[5] = {0};
        int k = 0;
        for (uint32_t i = start; i <= end && k < 4; i++, k++) {
            year_str[k] = uc[i];
        }

        uint32_t year_nr = atoi(year_str);

        if (year_nr >= 1800 && year_nr <= 2018) {
            strcpy(year, year_str);
            ret = 1;
        }
    }

    uregex_close(regEx);

    free(uc);

    return ret;
}

int extract_volume(uint8_t *text, uint8_t *volume) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);
    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "\\b(?i:volume|vol|v)\\.?[\\s:-]\\s*(\\d+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        if (end - start <= 4) {
            ucnv_fromUChars(conv, volume, VOLUME_LEN, uc + start, end - start, &uStatus);
        }

        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int extract_issue(uint8_t *text, uint8_t *issue) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "\\b(?i:issue|num|no|number|n)\\.?[\\s:-]\\s*(\\d+)";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);

    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        if (end - start <= 4) {
            ucnv_fromUChars(conv, issue, ISSUE_LEN, uc + start, end - start, &uStatus);
        }

        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}


int extract_issn(uint8_t *text, uint8_t *issn) {
    uint32_t ret = 0;
    UErrorCode errorCode = U_ZERO_ERROR;

    uint32_t text_len = strlen(text);

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);

    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "ISSN:?\\s*(\\d{4}[-]\\d{3}[\\dX])";
    UErrorCode uStatus = U_ZERO_ERROR;
    UBool isMatch;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);
    isMatch = uregex_find(regEx, 0, &uStatus);
    if (isMatch) {
        int32_t start = uregex_start(regEx, 1, &uStatus);
        int32_t end = uregex_end(regEx, 1, &uStatus);

        ucnv_fromUChars(conv, issn, target_len, uc + start, end - start, &uStatus);
        ret = 1;
    }

    uregex_close(regEx);
    free(uc);
    return ret;
}

int extract_journal(uint8_t *text, uint8_t *journal) {
    UErrorCode errorCode = U_ZERO_ERROR;
    uint32_t text_len = strlen(text);
    UConverter *conv = ucnv_open("UTF-8", &errorCode);
    int32_t target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc = malloc(target_len);
    ucnv_toUChars(conv, uc, target_len, text, text_len, &errorCode);

    URegularExpression *regEx;
    const char regText[] = "([\\p{Alphabetic}'.]+\\s)*[\\p{Alphabetic}'.]+";
    UErrorCode uStatus = U_ZERO_ERROR;

    regEx = uregex_openC(regText, 0, NULL, &uStatus);
    uregex_setText(regEx, uc, -1, &uStatus);

    while (uregex_findNext(regEx, &uStatus)) {
        int32_t start = uregex_start(regEx, 0, &uStatus);
        int32_t end = uregex_end(regEx, 0, &uStatus);

        uint8_t res[2048];
        ucnv_fromUChars(conv, res, sizeof(res), uc + start, end - start, &uStatus);

        uint8_t *s = res;

        uint8_t *e;

        uint32_t tokens_num = 0;

        while (*s) {
            while (*s == ' ') s++;
            e = s;
            while (*e != ' ' && *e != 0) e++;
            tokens_num++;
            s = e;
        }

        if (tokens_num < 2) continue;

        uint8_t processed_res[MAX_LOOKUP_TEXT_LEN];
        uint32_t processed_res_len = MAX_LOOKUP_TEXT_LEN;
        text_process(res, processed_res, &processed_res_len);

        uint64_t title_hash = text_hash64(processed_res, processed_res_len);
        if (journal_has(title_hash)) {
            strcpy(journal, res);
        }
    }

    uregex_close(regEx);
    free(uc);
}
