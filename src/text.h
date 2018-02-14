#ifndef RECOGNIZER_SERVER_TEXT_H
#define RECOGNIZER_SERVER_TEXT_H

typedef struct text_info {
    uint32_t  uppercase;
    uint32_t lowercase;
    uint32_t alphabetic;
    uint32_t numbers;
    uint32_t symbols;
} text_info_t;

uint32_t text_init();

uint32_t text_process(uint8_t *text, uint8_t *output_text, uint32_t *output_text_len);

uint32_t text_hash32(uint8_t *text, uint32_t text_len);

uint64_t text_hash64(uint8_t *text, uint32_t text_len);

uint32_t text_char_len(uint8_t *text);

text_info_t text_get_info(uint8_t *text);

uint32_t text_normalize_doi(uint8_t *doi);

uint32_t get_alphabetic_percent(uint8_t *text);

#endif //RECOGNIZER_SERVER_TEXT_H
