
#ifndef RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H
#define RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H

int extract_doi(uint8_t *text, uint8_t *doi);

int extract_isbn(uint8_t *text, uint8_t *isbn);

int extract_arxiv(uint8_t *text, uint8_t *arxiv);

int extract_year(uint8_t *text, uint8_t *year);

int extract_volume(uint8_t *text, uint8_t *volume);

int extract_issue(uint8_t *text, uint8_t *issue);

int extract_issn(uint8_t *text, uint8_t *issn);

int extract_journal(uint8_t *text, uint8_t *journal);


#endif //RECOGNIZER_SERVER_RECOGNIZE_VARIOUS_H
