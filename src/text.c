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

#include <jemalloc/jemalloc.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <string.h>
#include "log.h"

#define XXH_STATIC_LINKING_ONLY

#include "xxhash.h"
#include "text.h"

UNormalizer2 *unorm2;

uint32_t text_init() {
    UErrorCode status = U_ZERO_ERROR;
    unorm2 = unorm2_getNFKDInstance(&status);
    if (status != U_ZERO_ERROR) {
        log_error("unorm2_getNFKDInstance failed, error=%s", u_errorName(status));
        return 0;
    }
    return 1;
}

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    *output_text_len = 0;

    int32_t output_text_offset = 0;

    int32_t si, i = 0;
    UChar32 ci;

    UBool error = 0;
    uint8_t prev_new = 1;
    uint8_t prev_new_page = 1;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

        si = i;

        U8_NEXT(text, i, -1, ci);
        if (u_isUAlphabetic(ci)) {


            UChar uc[16];
            int32_t res = unorm2_getDecomposition(unorm2, ci, uc, 16, &status);

            if (res > 0) {
                if (status != U_ZERO_ERROR) {
                    //log_error("unorm2_getDecomposition error: %s", u_errorName(status));
                    return 0;
                }

                char decomposed_str[16] = {0};
                int32_t decomposed_str_len = 0;

                u_strToUTF8(decomposed_str, 16, &decomposed_str_len, uc, -1, &status);
                if (status != U_ZERO_ERROR) {
                    //log_error("u_strToUTF8 error: %s", u_errorName(status));
                    return 0;
                }

                int32_t j = 0;
                UChar32 cj;

                do {
                    if (output_text_offset >= max_output_text_len) {
                        error = 1;
                        break;
                    }

                    U8_NEXT(decomposed_str, j, decomposed_str_len, cj);
                    if (u_isUAlphabetic(cj)) {
                        cj = u_tolower(cj);
                        U8_APPEND(output_text, output_text_offset, max_output_text_len, cj, error);
                        if (error) break;
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);

                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;
            }
        } else if (u_getIntPropertyValue(ci, UCHAR_LINE_BREAK) == U_LB_LINE_FEED) {

        } else if (ci == '\f') {

        }
    } while (ci > 0);

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    return 1;
}

uint32_t text_process_fieldn(uint8_t *text, uint32_t text_len,
                             uint8_t *output_text, uint32_t *output_text_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    int32_t output_text_offset = 0;

    int32_t si, i = 0;
    UChar32 ci;

    UBool error = 0;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

        if (i >= text_len) break;

        si = i;
        U8_NEXT(text, i, -1, ci);
        if (u_isUAlphabetic(ci)) {
            UChar uc[16];
            int32_t res = unorm2_getDecomposition(unorm2, ci, uc, 16, &status);

            if (res > 0) {
                if (status != U_ZERO_ERROR) {
                    log_error("unorm2_getDecomposition error: %s", u_errorName(status));
                    return 0;
                }

                char decomposed_str[16] = {0};
                int32_t decomposed_str_len = 0;

                u_strToUTF8(decomposed_str, 16, &decomposed_str_len, uc, -1, &status);
                if (status != U_ZERO_ERROR) {
                    log_error("u_strToUTF8 error: %s", u_errorName(status));
                    return 0;
                }

                UChar32 cj;
                int32_t j = 0;

                do {
                    if (output_text_offset >= max_output_text_len) {
                        error = 1;
                        break;
                    }
                    U8_NEXT(decomposed_str, j, decomposed_str_len, cj);
                    if (u_isUAlphabetic(cj)) {
                        cj = u_tolower(cj);
                        U8_APPEND(output_text, output_text_offset, max_output_text_len, cj, error);
                        if (error) break;
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);
                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;
            }
        }
    } while (ci > 0);

    if (error) return 0;

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    return 1;
}

uint32_t text_hash32(uint8_t *text, uint32_t text_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, text, text_len);
    return (XXH64_digest(&state64)) >> 32;
}

uint64_t text_hash64(uint8_t *text, uint32_t text_len) {
    XXH64_state_t state64;
    XXH64_reset(&state64, 0);
    XXH64_update(&state64, text, text_len);
    return (XXH64_digest(&state64));
}

text_info_t text_get_info(uint8_t *text) {
    text_info_t text_info;
    memset(&text_info, 0, sizeof(text_info_t));

    uint32_t s, i = 0;
    UChar32 c;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (!c) break;

        if (u_isUUppercase(c)) text_info.uppercase++;
        if (u_isULowercase(c)) text_info.lowercase++;
        if (u_isUAlphabetic(c)) text_info.alphabetic++;

    } while (1);

    return text_info;
}

uint32_t get_alphabetic_percent(uint8_t *text) {

    uint32_t total = 0;

    uint32_t alphabetic = 0;

    uint32_t s, i = 0;
    UChar32 c;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (!c) break;

        total++;

        if (u_isUAlphabetic(c)) alphabetic++;

    } while (1);

    return alphabetic*100/total;
}