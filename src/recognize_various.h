
#ifndef RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H
#define RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H

uint32_t extract_doi(uint8_t *text, uint8_t *doi);

uint32_t extract_isbn(uint8_t *text, uint8_t *isbn);

uint32_t extract_arxiv(uint8_t *text, uint8_t *arxiv);

uint32_t extract_year(uint8_t *text, uint8_t *year);

uint32_t extract_volume(uint8_t *text, uint8_t *volume);

uint32_t extract_issue(uint8_t *text, uint8_t *issue);

uint32_t extract_issn(uint8_t *text, uint8_t *issn);

uint32_t extract_journal(uint8_t *text, uint8_t *journal);

#endif //RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H
