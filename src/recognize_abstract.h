
#ifndef RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H
#define RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H

uint32_t extract_abstract_simple(page_t *page, uint8_t *abstract, uint32_t abstract_size);

uint32_t extract_abstract_structured(page_t *page, uint8_t *abstract, uint32_t abstract_size);

#endif //RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H
