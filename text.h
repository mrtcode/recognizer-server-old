#ifndef RECOGNIZER_SERVER_TEXT_H
#define RECOGNIZER_SERVER_TEXT_H

typedef struct line {
    uint32_t start;
    uint32_t end;
} line_t;

uint32_t text_init();

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len,
                      uint32_t *map, uint32_t *map_len, line_t *lines, uint32_t *lines_len);

uint32_t text_process_field(uint8_t *text, uint8_t *output_text,
                            uint32_t *output_text_len, uint8_t multi);

uint32_t text_hash28(uint8_t *text, uint32_t text_len);

uint64_t text_hash56(uint8_t *text, uint32_t text_len);

uint32_t text_raw_title(uint8_t *text, uint32_t *map, uint32_t map_len,
                        uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max);

uint32_t text_raw_name(uint8_t *text, uint32_t *map, uint32_t map_len,
                       uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max);

uint32_t text_raw_abstract(uint8_t *text, uint32_t *map, uint32_t map_len,
                           uint32_t start, uint32_t end, uint8_t *str, uint32_t str_len_max);

uint32_t text_hash32(uint8_t *text, uint32_t text_len);

uint64_t text_hash64(uint8_t *text, uint32_t text_len);

uint64_t text_rh_get32(uint8_t *needle, uint32_t needle_len);
uint8_t *text_rh_find32(uint8_t *haystack, uint32_t haystack_len, uint32_t needle_hash, uint32_t needle_len);

#endif //RECOGNIZER_SERVER_TEXT_H
