
#ifndef RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H
#define RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H

int extract_abstract_simple(page_t *page, uint8_t *abstract, uint32_t abstract_size);

int extract_abstract_structured(page_t *page, uint8_t *abstract, uint32_t abstract_size);

#endif //RECOGNIZER_SERVER_RECOGNIZE_ABSTRACT_H
