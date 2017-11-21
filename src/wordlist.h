#ifndef RECOGNIZER_SERVER_WORDLIST_H
#define RECOGNIZER_SERVER_WORDLIST_H

#define WORDLIST_ERROR 0
#define WORDLIST_SUCCESS 1

int wordlist_init();

int wordlist_save();

uint8_t wordlist_add(uint64_t h, uint32_t aa, uint32_t bb, uint32_t cc);

uint8_t wordlist_get(uint64_t h, uint32_t *a, uint32_t *b, uint32_t *c);

#endif //RECOGNIZER_SERVER_WORDLIST_H
