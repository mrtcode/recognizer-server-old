#ifndef RECOGNIZER_SERVER_JOURNAL_H
#define RECOGNIZER_SERVER_JOURNAL_H

uint32_t journal_init(uint8_t *directory);

uint8_t journal_add(uint64_t h);

uint8_t journal_has(uint64_t h);

#endif //RECOGNIZER_SERVER_JOURNAL_H
