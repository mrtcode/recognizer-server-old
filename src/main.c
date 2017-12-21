/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2017 Zotero
 https://www.zotero.org

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 ***** END LICENSE BLOCK *****
 */

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <onion/onion.h>
#include <onion/block.h>
#include <onion/exportlocal.h>
#include <jansson.h>
#include <unicode/utf.h>
#include <sqlite3.h>
#include <dirent.h>
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "rh.h"
#include "log.h"
#include "wordlist.h"
#include "journal.h"

onion *on = NULL;
pthread_rwlock_t data_rwlock;
pthread_rwlock_t saver_rwlock;

uint8_t indexing_mode = 0;

json_t *authors_to_json(uint8_t *authors) {
    json_t *json_authors = json_array();
    uint8_t *p = authors;
    uint8_t *s;

    uint8_t *first_name = 0, *last_name = 0;
    uint32_t first_name_len = 0, last_name_len = 0;

    while (1) {
        while (*p == '\t' || *p == '\n') p++;
        if (!*p) break;
        s = p;
        while (*p && *p != '\t' && *p != '\n') p++;

        if (*p == '\t') {
            first_name = s;
            first_name_len = p - s;
        } else {
            last_name = s;
            last_name_len = p - s;
            json_t *json_author = json_object();
            json_object_set(json_author, "firstName", json_stringn(first_name, first_name_len));
            json_object_set(json_author, "lastName", json_stringn(last_name, last_name_len));
            json_array_append(json_authors, json_author);
            first_name = 0;
            last_name = 0;
        }

        if (!*p) break;
    }
    return json_authors;
}

int save_json(uint8_t *data) {
    char buff[100];
    time_t now = time (0);
    strftime (buff, 100, "%Y-%m-%d %H:%M:%S", localtime (&now));
    printf("%s\n", buff);

    char filename[1024];
    sprintf(filename, "./json/%s.%u.json", buff, rand());

    FILE *f = fopen(filename, "w");
    if (f == NULL)
    {
        printf("Error opening file!\n");
        exit(1);
    }

    fwrite(data, strlen(data), 1, f);

    fclose(f);

    return 0;
}

onion_connection_status url_recognize(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);

    save_json(data);

    json_t *root;
    json_error_t error;
    root = json_loads(data, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    json_t *json_body = json_object_get(root, "body");

    if (!json_is_object(json_body)) {
        return OCS_PROCESSED;;
    }

    struct timeval st, et;

    res_metadata_t result = {0};
    uint32_t rc;
    pthread_rwlock_rdlock(&data_rwlock);

    gettimeofday(&st, NULL);
    rc = recognize(json_body, &result);
    gettimeofday(&et, NULL);

    pthread_rwlock_unlock(&data_rwlock);

    uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();

    json_object_set_new(obj, "time", json_integer(elapsed));
//    if (0) {
        if(*result.type!=0) json_object_set(obj, "type", json_string(result.type));
        json_object_set(obj, "title", json_string(result.title));
        json_object_set(obj, "authors", authors_to_json(result.authors));
        if(*result.doi!=0) json_object_set(obj, "doi", json_string(result.doi));
        if(*result.isbn!=0) json_object_set(obj, "isbn", json_string(result.isbn));
        if(*result.arxiv!=0) json_object_set(obj, "arxiv", json_string(result.arxiv));
        if(*result.abstract!=0) json_object_set(obj, "abstract", json_string(result.abstract));
        if(*result.year!=0) json_object_set(obj, "year", json_string(result.year));
        if(*result.container!=0) json_object_set(obj, "container", json_string(result.container));
        if(*result.publisher!=0) json_object_set(obj, "publisher", json_string(result.publisher));
        if(*result.pages!=0) json_object_set(obj, "pages", json_string(result.pages));
        if(*result.volume!=0) json_object_set(obj, "volume", json_string(result.volume));
        if(*result.issue!=0) json_object_set(obj, "issue", json_string(result.issue));
        if(*result.issn!=0) json_object_set(obj, "issn", json_string(result.issue));
        if(*result.url!=0) json_object_set(obj, "url", json_string(result.url));
//    }

    printf("type: %s\ntitle: %s\nauthors: %s\ndoi: %s\nisbn: %s\narxiv: %s\nyear: %s\ncontainer: %s\npublisher: %s\nabstract: %s\npages: %s\nvolume: %s\nissue: %s\nissn: %s\n",
           result.type,
           result.title,
           result.authors,
           result.doi,
           result.isbn,
           result.arxiv,
           result.year,
           result.container,
           result.publisher,
           result.abstract,
           result.pages,
           result.volume,
           result.issue,
           result.issn
    );

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, "%s", str);
    free(str);

    return OCS_PROCESSED;
}

onion_connection_status url_index2(void *_, onion_request *req, onion_response *res) {
    if (onion_request_get_flags(req) & OR_POST) {
        struct timeval st, et;

        gettimeofday(&st, NULL);
        const onion_block *dreq = onion_request_get_data(req);

        if (!dreq) return OCS_PROCESSED;

        const char *data = onion_block_data(dreq);

        json_t *root;
        json_error_t error;

        root = json_loads(data, 0, &error);

        if (!root) {
            return OCS_PROCESSED;
        }

        uint32_t indexed = 0;
        if (json_is_array(root)) {
            uint32_t n = json_array_size(root);
            for (uint32_t i = 0; i < n; i++) {
                json_t *el = json_array_get(root, i);
                if (json_is_object(el)) {
                    json_t *json_title = json_object_get(el, "title");
                    json_t *json_authors = json_object_get(el, "authors");
                    json_t *json_doi = json_object_get(el, "doi");

                    metadata_t metadata;
                    memset(&metadata, 0, sizeof(metadata_t));

                    if (json_is_string(json_title) &&
                        json_is_string(json_authors) &&
                        json_is_string(json_doi)) {

                        uint8_t *title = json_string_value(json_title);
                        uint8_t *authors = json_string_value(json_authors);
                        uint8_t *doi = json_string_value(json_doi);

                        if (index_metadata2(title, authors, doi)) {
                            indexed++;
                        }
                    }
                }
            }
        }

        json_decref(root);

        gettimeofday(&et, NULL);

        uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

        json_t *obj = json_object();
        json_object_set_new(obj, "time", json_integer(elapsed));
        json_object_set_new(obj, "indexed", json_integer(indexed));

        char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
        json_decref(obj);

        onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
        onion_response_printf(res, "%s", str);
        free(str);
    }

    return OCS_PROCESSED;;
}

onion_connection_status url_stats(void *_, onion_request *req, onion_response *res) {
    stats_t stats = ht_stats();
    json_t *obj = json_object();
    json_object_set(obj, "used_hashes", json_integer(stats.used_rows));
    json_object_set(obj, "total_ah_slots", json_integer(stats.total_ah_slots));
    json_object_set(obj, "total_th_slots", json_integer(stats.total_th_slots));
    json_object_set(obj, "max_ah_slots", json_integer(stats.max_ah_slots));
    json_object_set(obj, "max_th_slots", json_integer(stats.max_th_slots));

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, "%s", str);
    free(str);

    return OCS_PROCESSED;
}

int save() {
    pthread_rwlock_wrlock(&saver_rwlock);
    pthread_rwlock_rdlock(&data_rwlock);
    log_info("saving");
    db_dois_save();
    ht_save();
    log_info("saved");
    pthread_rwlock_unlock(&data_rwlock);
    pthread_rwlock_unlock(&saver_rwlock);
}

void *saver_thread(void *arg) {
    uint64_t saver_last_total = 0;
    uint64_t indicator_last_total = 0;
    time_t indicator_t = 0;

    while (1) {
        usleep(50000);

        // Force flush because otherwise Docker doesn't output logs
        fflush(stdout);
        fflush(stderr);

        time_t t = time(0);

        uint64_t current_total_indexed = index_total_indexed();

        if (current_total_indexed > indicator_last_total) {
            if (indicator_t + 30 <= t) {
                log_info("indexed total=%u, per_second=%u",
                         current_total_indexed,
                         (current_total_indexed - indicator_last_total) / (t - indicator_t));
                indicator_last_total = current_total_indexed;
                indicator_t = t;
            }
        } else {
            indicator_t = t;
        }

        if (!indexing_mode) {
            if (current_total_indexed > saver_last_total) {
                if (t - index_updated_t() >= 10) {
                    save();
                    saver_last_total = current_total_indexed;
                }
            }

            if (db_dois_in_transaction() >= 50000000) {
                save();
            }
        }
    }
}

void signal_handler(int signum) {
    log_info("signal received (%d), shutting down..", signum);

    pthread_rwlock_wrlock(&data_rwlock);

    if (on) {
        onion_listen_stop(on);
    }

    log_info("saving");
    ht_save();
    db_dois_save();
    log_info("saved");

    if (!db_close()) {
        log_error("db close failed");
//        return;
    }

    log_info("exiting");

    // Force flush because otherwise Docker doesn't output logs
    fflush(stdout);
    fflush(stderr);
    exit(EXIT_SUCCESS);
}

void print_usage() {
    printf(
            "Missing parameters.\n" \
            "-d\tdata directory\n" \
            "-p\tport\n" \
            "-i\tstart in indexing mode\n" \
            "Usage example:\n" \
            "recognizer-server -d /var/db -p 8080\n"
    );
}
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <unicode/uregex.h>

UChar *touc(const char *text, int32_t text_len) {
    UErrorCode errorCode = U_ZERO_ERROR;
    int32_t target_len;

    UConverter *conv = ucnv_open("UTF-8", &errorCode);

    target_len = UCNV_GET_MAX_BYTES_FOR_STRING(text_len, ucnv_getMaxCharSize(conv));
    UChar *uc1 = malloc(target_len);

    ucnv_toUChars(conv, uc1, text_len, text, text_len, &errorCode);

    return uc1;
}



int main(int argc, char **argv) {
    char *opt_db_directory = 0;
    char *opt_port = 0;

    int opt;
    while ((opt = getopt(argc, argv, "d:p:i")) != -1) {
        switch (opt) {
            case 'd':
                opt_db_directory = optarg;
                break;
            case 'p':
                opt_port = optarg;
                break;
            case 'i':
                indexing_mode = 1;
                break;
            default:
                print_usage();
                return EXIT_FAILURE;
        }
    }

    if (!opt_db_directory || !opt_port) {
        print_usage();
        return EXIT_FAILURE;
    }


//    DIR *dir = opendir(opt_db_directory);
//    if (!dir) {
//        log_error("database directory is invalid");
//        return EXIT_FAILURE;
//    }
//
//    indexing_mode = 1;
//
//    uint32_t n = 0;
//    while (readdir(dir)) {
//        n++;
//        if (n == 3) {
//            indexing_mode = 0;
//            break;
//        }
//    }
//    closedir(dir);

//    indexing_mode = 1;


    log_info("starting in normal mode");


    setenv("ONION_LOG", "noinfo", 1);
    pthread_rwlock_init(&data_rwlock, 0);
    pthread_rwlock_init(&saver_rwlock, 0);

    if (!text_init()) {
        log_error("failed to initialize text processor");
        return EXIT_FAILURE;
    }

//    UNormalizer2 *unorm2;
//
//        UErrorCode status = U_ZERO_ERROR;
//        unorm2 = unorm2_getNFKDInstance(&status);
//        if (status != U_ZERO_ERROR) {
//            log_error("unorm2_getNFKDInstance failed, error=%s", u_errorName(status));
//            return 0;
//        }
//
//
//
//    printf("alphabetic: %d %d\n",unorm2_composePair(unorm2, 97,778), u_charType(778));
//
//return 0;

    journal_init();

//    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
//    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
//    text_process("internationaljournalofengineeringandtechnology", output_text, &output_text_len, 0, 0);
//
//
//    uint64_t title_hash = text_hash64(output_text, output_text_len);
//
//    uint8_t jj = journal_has(title_hash);
//
//    if(jj) {
//        printf("found\n");
//    }



    wordlist_init();

    //test_authors();
    //return 0;

    if (!db_normal_mode_init(opt_db_directory)) {
        log_error("failed to initialize db normal mode");
        return EXIT_FAILURE;
    }


//    uint8_t txt[] = "aasdfasdfasdbgrt  https://doi.org/10.1007/BF01971386 werwe";
//
//    uint8_t doi[1024];
//
//
//    extract_doi(txt, doi, 1000);
//
//    printf("doi: %s\n", doi);
//
//
//    uint8_t txt[] = "aasdfasdfasdbgrt  ISBN 978-1-78328-731-4 werwe";
//
//    uint8_t doi[1024];
//
//
//    extract_isbn(txt, doi);
//
//    printf("doi: %s\n", doi);


//    UChar *uc = touc(txt, strlen(txt));
//
//    URegularExpression *regEx;
//    const char regText[]="10(?:\\.[0-9]{4,})?\\/[^\\s]*[^\\s\\.,]";
//    UErrorCode uStatus= U_ZERO_ERROR;
//    UBool isMatch;
//
//    printf("regex = %s\n",regText);
//    regEx=uregex_openC(regText,0, NULL, &uStatus);
//    uregex_setText (regEx, uc, -1, &uStatus);
//    isMatch = uregex_find(regEx, 0, &uStatus);
//    if (!isMatch){
//        int32_t failPos=uregex_end(regEx, 0, &uStatus);
//        printf( "No match at position %d\n",failPos);
//    }
//    else {
//        printf("Pattern matches\n");
//        int nn = uregex_start(regEx, 0, &uStatus);
//        printf("nn: %d\n", nn);
//        printf("%s\n", txt+nn);
//    }
//
//
//    uregex_close(regEx);

//    return 0;


    if (!ht_init()) {
        log_error("failed to initialize hashtable");
        return EXIT_FAILURE;
    }

//    index_metadata2("renaleharnstoffundelektrolytkonzentrationsprofdebeiexperimentellerasymmetrischerglomerulonephritis",
//    "Michel\tAbdalla\nBerlin\tGermany\n",
//                    "10.0000/asdf"
//    );
//
//   index_metadata2("Renale Harnstoff- und Elektrolytkonzentrationsprofile bei experimenteller asymmetrischer Glomerulonephritis",
//                   "L. J.\tHeuer\nH. J.\tLudwig",
//                   "10.1007/bf01971386"
//   );
//
//    uint8_t *tt = "renaleharnstoffundelektrolytkonzentrationsprofdebeiexperimentellerasymmetrischerglomerulonephritis";
//    uint64_t title_hash = text_hash64(tt, strlen(tt));
//    //printf("Lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);
//
//    slot_t *slots[100];
//    uint32_t slots_len;
//
//    ht_get_slots(title_hash, slots, &slots_len);
//    if (slots_len) {
//        printf("fffffff\n");
//    }
////
//    return 0;


        stats_t stats = ht_stats();
    log_info("\nused_rows=%u\ntotal_ah_slots=%u\ntotal_th_slots=%u\nmax_ah_slots=%u\nmax_th_slots=%u\n",
             stats.used_rows, stats.total_ah_slots, stats.total_th_slots, stats.max_ah_slots, stats.max_th_slots);

    pthread_t tid;
    pthread_create(&tid, NULL, saver_thread, 0);

    on = onion_new(O_POOL);

    // Signal handler must be initialized after onion_new
    struct sigaction action;
    memset(&action, 0, sizeof(struct sigaction));
    action.sa_handler = signal_handler;
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    onion_set_port(on, opt_port);
    onion_set_max_threads(on, 16);
    onion_set_max_post_size(on, 50000000);

    onion_url *urls = onion_root_url(on);

    if(!indexing_mode) onion_url_add(urls, "recognize", url_recognize);
    onion_url_add(urls, "index2", url_index2);
    onion_url_add(urls, "stats", url_stats);
//    onion_url_add_handler(urls, "panel", onion_handler_export_local_new("static/panel.html"));

    log_info("listening on port %s", opt_port);

    onion_listen(on);

    onion_free(on);
    return EXIT_SUCCESS;
}
