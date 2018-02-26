#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>
#include <unicode/ustdio.h>
#include "recognize.h"
#include "log.h"
#include "recognize_abstract.h"

uint32_t is_simple_abstract_name(uint8_t *text) {
    static uint8_t names[10][32] = {
            "abstract",
            "summary"
    };

    for (uint32_t i = 0; i < 2; i++) {
        uint8_t *c = names[i];
        uint8_t *v = text;

        uint8_t first = 1;

        while (*c) {
            if (first) {
                if (*c - 32 != *v) break;
            } else {
                if (*c != *v && *c - 32 != *v) break;
            }
            first = 0;
            c++;
            v++;
        }

        if (!*c) {
            log_debug("found abstract name: %s\n", names[i]);
            return c - names[i];
        }
    }
    return 0;
}

uint32_t is_dot_last(uint8_t *text) {
    uint8_t *c = &text[strlen(text) - 1];

    while (c >= text) {
        if (*c == ' ' || *c == '\n' || *c == '\r') {

        } else {
            if (*c == '.') {
                return 1;
            } else {
                return 0;
            }
        }
        c--;
    }

    return 0;
}

uint32_t extract_abstract_simple(page_t *page, uint8_t *abstract, uint32_t abstract_size) {
    uint8_t found_abstract = 0;

    uint8_t start = 0;
    uint32_t start_skip = 0;
    uint8_t finish = 0;

    uint32_t abstract_len = 0;

    UBool error = 0;

    double abstract_x_min = 0;
    double abstract_x_max = 0;

    double txt_x_min = 0;
    double txt_x_max = 0;

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                if (!found_abstract) {
                    uint32_t name_len = is_simple_abstract_name(line->words[0].text);
                    if (name_len) {
                        found_abstract = 1;
                        start = 1;
                        start_skip = name_len;
                        abstract_x_min = line->words[0].x_min;
                        abstract_x_max = line->words[0].x_max;
                    }
                }

                if (start) {
                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        uint32_t s, i = 0;
                        UChar32 c;

                        do {
                            s = i;
                            U8_NEXT(word->text, i, -1, c);

                            if (!c) break;

                            if (start_skip) {
                                start_skip--;
                            } else {

                                if (!finish && (u_isUAlphabetic(c) || u_isdigit(c))) {
                                    finish = 1;
                                }

                                if (finish) {

                                    if (!txt_x_min || txt_x_min > word->x_min) {
                                        txt_x_min = word->x_min;
                                    }

                                    if (!txt_x_max || txt_x_max < word->x_max) {
                                        txt_x_max = word->x_max;
                                    }
//                                    if(abstract_x_max && word->x_min > abstract_x_max) {
//                                        return 0;
//                                    }

                                    U8_APPEND(abstract, abstract_len, abstract_size - 1, c, error);
                                    if (error) {
                                        *abstract = 0;
                                        log_error("unorm2_getNFKDInstance failed, error=%s", u_errorName(error));
                                        return 0;
                                    }

                                    abstract[abstract_len] = 0;
                                }
                            }
                        } while (1);

                        if (word->space) {
                            if (start_skip) {
                                start_skip--;
                            } else {
                                if (finish) {
                                    if (word->space) {
                                        abstract[abstract_len++] = ' ';
                                        abstract[abstract_len] = 0;
                                    }
                                }
                            }
                        }
                    }
                }

                if (finish) {
                    if (abstract_len && abstract[abstract_len - 1] == '-') {
                        abstract_len--;
                        abstract[abstract_len] = 0;
                    } else {
                        abstract[abstract_len++] = ' ';
                        abstract[abstract_len] = 0;

                    }
                }


                if (finish) {

                    log_debug("line: %f\n", line->x_max);
                    if (is_dot_last(abstract) &&
                        line_i >= 2 &&
                        fabs(block->lines[line_i - 2].x_max - block->lines[line_i - 1].x_max) < 1.0 &&
                        block->lines[line_i].x_max < block->lines[line_i - 1].x_max - 2) {
                        log_debug("%s\n\n\n", abstract);
                        abstract[abstract_len] = 0;

                        if (abstract_x_max > txt_x_max || abstract_x_max < txt_x_min) {
                            return 0;
                        }
                        return 1;
                    }
                }
            }

            if (finish) {
//                log_debug("%s\n\n\n", abstract);
                if (!is_dot_last(abstract)) continue;
                abstract[abstract_len] = 0;

                if (abstract_x_max > txt_x_max || abstract_x_max < txt_x_min) {
                    return 0;
                }
                return 1;
            }
        }

        if (finish) {
            log_debug("%s\n\n\n", abstract);
            abstract[abstract_len] = 0;

            if (abstract_x_max > txt_x_max || abstract_x_max < txt_x_min) {
                return 0;
            }
            return 1;
        }
    }

    *abstract = 0;
    return 0;
}

uint32_t is_structured_abstract_name(uint8_t *text) {
    static uint8_t names[11][32] = {
            "background",
            "methods",
            "method",
            "conclusions",
            "conclusion",
            "objectives",
            "objective",
            "results",
            "result",
            "purpose",
            "measurements"
    };

    uint32_t types[11] = {
            1,
            2,
            2,
            3,
            3,
            4,
            4,
            5,
            5,
            6,
            7
    };

    for (uint32_t i = 0; i < 11; i++) {
        uint8_t *c = names[i];
        uint8_t *v = text;

        uint8_t first = 1;

        while (*c) {
            if (first) {
                if (*c - 32 != *v) break;
            } else {
                if (*c != *v && *c - 32 != *v) break;
            }
            first = 0;
            c++;
            v++;
        }

        if (!*c) {
            log_debug("found name: %s\n", names[i]);
            return types[i];
        }
    }
    return 0;
}

uint32_t extract_abstract_structured(page_t *page, uint8_t *abstract, uint32_t abstract_size) {
    uint8_t start = 0;
    uint32_t abstract_len = 0;
    uint8_t exit = 0;
    UBool error = 0;

    uint32_t names_detected = 0;

    double x_min = 0;
    double font_size = 0;

    uint32_t last_name_type = 0;

    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                uint32_t type = is_structured_abstract_name(line->words[0].text);
                if (type) {
                    last_name_type = type;
                    names_detected++;
                    start = 1;
                    exit = 0;
                    if (abstract_len) abstract[abstract_len++] = '\n';

                    if (x_min) {
                        if (fabs(x_min - line->words[0].x_min) > 2) {
                            return 0;
                        }
                    } else {
                        x_min = line->words[0].x_min;
                    }

                    if (font_size) {
                        if (fabs(font_size - line->words[0].font_size) > 1) {
                            return 0;
                        }
                    } else {
                        font_size = line->words[0].font_size;
                    }
                }

                if (start) {
                    if (exit) {
                        log_debug("%s\n\n\n", abstract);
                        *abstract = 0;
                        return 0;
                    }
                }

                if (start) {
                    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
                        word_t *word = line->words + word_i;

                        uint32_t s, i = 0;
                        UChar32 c;

                        do {
                            s = i;
                            U8_NEXT(word->text, i, -1, c);

                            if (!c) break;

                            U8_APPEND(abstract, abstract_len, abstract_size - 1, c, error);
                            if (error) {
                                *abstract = 0;
                                return 0;
                            }
                        } while (1);

                        if (word->space) {
                            abstract[abstract_len++] = ' ';
                        }

                    }
                }
            }

        }

        if (start) {
            if (names_detected >= 2 && last_name_type == 3) {
                log_debug("%s\n\n\n", abstract);
                abstract[abstract_len] = 0;
                return 1;
            } else {
                *abstract = 0;
                return 0;
            }
        }

    }

    return 0;
}

