#ifndef RECOGNIZER_SERVER_RECOGNIZE_H
#define RECOGNIZER_SERVER_RECOGNIZE_H

#include "defines.h"

typedef struct res_metadata {
    uint8_t type[TYPE_LEN];
    uint8_t title[TITLE_LEN + 1];
    uint8_t authors[AUTHORS_LEN + 1];
    uint8_t doi[DOI_LEN+1];
    uint8_t isbn[ISBN_LEN+1];
    uint8_t arxiv[ARXIV_LEN+1];
    uint8_t abstract[ABSTRACT_LEN+1];
    uint8_t container[CONTAINER_LEN+1];
    uint8_t publisher[PUBLISHER_LEN+1];
    uint8_t year[YEAR_LEN+1];
    uint8_t pages[PAGES_LEN+1];
    uint8_t volume[VOLUME_LEN+1];
    uint8_t issue[ISSUE_LEN+1];
    uint8_t issn[ISSN_LEN+1];
    uint8_t url[URL_LEN+1];
} res_metadata_t;

typedef struct pdf_metadata {
    uint8_t title[TITLE_LEN];
    uint8_t doi[DOI_LEN];
    uint8_t authors[AUTHORS_LEN];
} pdf_metadata_t;

uint32_t recognize(json_t *body, res_metadata_t *result);


#endif //RECOGNIZER_SERVER_RECOGNIZE_H
