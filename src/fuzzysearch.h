#ifndef RECOGNIZER_FUZZYSEARCH_H
#define RECOGNIZER_FUZZYSEARCH_H

int fuzzysearch(uint32_t *needle, uint32_t needle_len,
                uint32_t *haystack, uint32_t haystack_len,
                uint32_t *match_offset, uint32_t *match_distance,
                uint32_t max_distance);

#endif //RECOGNIZER_FUZZYSEARCH_H
