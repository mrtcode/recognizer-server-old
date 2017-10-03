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

#include <jemalloc/jemalloc.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <string.h>
#include "log.h"

#define XXH_STATIC_LINKING_ONLY

#include "xxhash.h"
#include "text.h"
#include "rh.h"

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

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len,
                      uint32_t *map, uint32_t *map_len, line_t *lines, uint32_t *lines_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    *output_text_len = 0;
    if (map) *map_len = 0;
    if (lines) *lines_len = 0;
    int32_t output_text_offset = 0;

    int32_t si, i = 0;
    UChar32 ci;

    UBool error = 0;
    uint8_t prev_new = 1;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

        si = i;

        U8_NEXT(text, i, -1, ci);
        if (u_isUAlphabetic(ci)) {
            if (lines) {
                if (prev_new) {
                    lines[*lines_len].start = output_text_offset;
                    (*lines_len)++;
                }
                prev_new = 0;
            }

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

                        if (map) {
                            while (*map_len < output_text_offset) {
                                map[(*map_len)++] = si;
                            }
                        }
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);

                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;

                if (map) {
                    while (*map_len < output_text_offset) {
                        map[(*map_len)++] = si;
                    }
                }
            }
        } else if (u_getIntPropertyValue(ci, UCHAR_LINE_BREAK) == U_LB_LINE_FEED) {
            if (lines) {
                if (!prev_new) {
                    lines[(*lines_len) - 1].end = output_text_offset - 1;
                }
                prev_new = 1;
            }
        }
    } while (ci > 0);

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    if (lines) {
        if (!prev_new) {
            lines[(*lines_len) - 1].end = *output_text_len - 1;
        }
    }

    return 1;
}

uint32_t text_process_field(uint8_t *text, uint8_t *output_text,
                            uint32_t *output_text_len, uint8_t multi) {
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
        } else if (multi && text[si] == ',') {
            output_text[output_text_offset++] = ',';
        }
    } while (ci > 0);

    if (error) return 0;

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

uint32_t text_raw_title(uint8_t *text, uint32_t *map, uint32_t map_len,
                        uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max) {
    uint32_t raw_start = map[start];
    uint32_t raw_end = map[end];

    uint8_t *p;
    uint8_t *u = str;

    uint32_t s, i = raw_start;
    UChar32 c;

    uint8_t prev_white = 0;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (u_isWhitespace(c)) {
            if (!prev_white) {
                *u++ = ' ';
            }
            prev_white = 1;
        } else {
            prev_white = 0;
            for (uint32_t j = s; j < i; j++) {
                *u++ = *(text + j);
            }
        }
    } while (c > 0 && i <= raw_end);
    *u = 0;
    return 1;
}

uint32_t text_raw_name(uint8_t *text, uint32_t *map, uint32_t map_len,
                       uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max) {
    uint32_t raw_start = map[start];
    uint32_t raw_end = map[end];

    uint8_t *p;
    uint8_t *u = str;

    uint32_t s, i = raw_start;
    UChar32 c;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (!u_isWhitespace(c)) {
            for (uint32_t j = s; j < i; j++) {
                *u++ = *(text + j);
            }
        }
    } while (c > 0 && i <= raw_end);
    *u = 0;
    return 1;
}

uint32_t text_raw_abstract(uint8_t *text, uint32_t *map, uint32_t map_len,
                           uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max) {
    uint32_t raw_start = map[start];
    uint32_t raw_end = map[end];

    uint8_t *p;
    uint8_t *u = str;

    uint32_t s, i = raw_start;
    UChar32 c;

    uint8_t prev_white = 0;

    do {
        s = i;
        U8_NEXT(text, i, -1, c);

        if (u_isWhitespace(c)) {
            if (!prev_white) {
                *u++ = ' ';
            }
            prev_white = 1;
        } else {
            prev_white = 0;
            for (uint32_t j = s; j < i; j++) {
                *u++ = *(text + j);
            }
        }
    } while (c > 0 && i <= raw_end);
    *u = 0;
    return 1;
}

uint64_t text_rh_get32(uint8_t *needle, uint32_t needle_len) {
    rh_state_t state;
    rh_reset(&state);

    for (uint32_t i = 0; i < needle_len; i++) {
        rh_rollin(&state, needle[i]);
    }
    return rh_digest(&state) >> 32;
}

uint8_t *text_rh_find32(uint8_t *haystack, uint32_t haystack_len, uint32_t needle_hash, uint32_t needle_len) {
    rh_state_t state;
    rh_reset(&state);

    for (uint32_t i = 0; i < needle_len; i++) {
        rh_rollin(&state, haystack[i]);
    }

    if (rh_digest(&state) == needle_hash) return haystack;

    for (uint32_t i = 0; i + needle_len + 1 < haystack_len; i++) {
        rh_rotate(&state, *(haystack + i + needle_len), *(haystack + i));

        if (rh_digest(&state) >> 32 == needle_hash) return haystack + i + 1;
    }

    return 0;
}


uint32_t text_hashable_author(uint8_t *text, uint32_t text_len,
                              uint8_t *output_text, uint32_t *output_text_len) {
    UErrorCode status = U_ZERO_ERROR;
    int32_t max_output_text_len = *output_text_len - 1;
    int32_t output_text_offset = 0;

    int32_t si, i = 0;
    UChar32 ci;

    UBool error = 0;

    uint8_t first_character = 1;

    int32_t offset = 0;

    do {
        if (output_text_offset >= max_output_text_len) {
            error = 1;
            break;
        }

//        if (i >= text_len) break;

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
                        if (first_character) {
                            offset = output_text_offset;
                            first_character = 0;
                        }
                    }
                } while (cj > 0);
                if (error) break;
            } else {
                ci = u_tolower(ci);
                U8_APPEND(output_text, output_text_offset, max_output_text_len, ci, error);
                if (error) break;
                if (first_character) {
                    offset = output_text_offset;
                    first_character = 0;
                }
            }
        } else if (text[si] == '\n') {
            break;
        } else {
            output_text_offset = offset;
        }
    } while (ci > 0);

    if (error) return 0;

    output_text[output_text_offset] = 0;
    *output_text_len = output_text_offset;

    return 1;
}

uint64_t get_metadata_hash(uint8_t *title, uint8_t *authors) {
    uint8_t buf[2048];
    uint32_t buf_len = 1023 + 1;
    if(!text_process(title, buf, &buf_len, 0, 0, 0, 0)) return 0;

    uint32_t buf1_len = 255 + 1;
    if(!text_hashable_author(authors, 0, buf + buf_len, &buf1_len)) return 0;

    uint64_t metadata_hash = text_hash64(buf, buf_len + buf1_len);

    return metadata_hash;
}