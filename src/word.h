#ifndef RECOGNIZER_SERVER_WORDLIST_H
#define RECOGNIZER_SERVER_WORDLIST_H

uint32_t word_init(uint8_t *directory);

uint8_t word_add(uint64_t h, uint32_t aa, uint32_t bb, uint32_t cc);

uint8_t word_get(uint64_t h, uint32_t *a, uint32_t *b, uint32_t *c);

#endif //RECOGNIZER_SERVER_WORDLIST_H
