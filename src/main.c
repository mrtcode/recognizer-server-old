/*
 ***** BEGIN LICENSE BLOCK *****

 Copyright © 2018 Zotero
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
#include <zlib.h>
#include "ht.h"
#include "db.h"
#include "text.h"
#include "index.h"
#include "recognize.h"
#include "log.h"
#include "wordlist.h"
#include "journal.h"

int log_level = 1;

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
    time_t now = time(0);
    strftime(buff, 100, "%Y-%m-%d %H:%M:%S", localtime(&now));

    char filename[1024];
    sprintf(filename, "./json/%s.%u.json", buff, rand());

    FILE *f = fopen(filename, "w");
    if (f == NULL) {
        log_error("Error opening file!\n");
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

    const char *content_encoding = onion_request_get_header(req, "Content-Encoding");

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);
    uint32_t data_len = onion_block_size(dreq);

    char *d = data;

    char *uncompressed_data = 0;

    if (content_encoding && !strcmp(content_encoding, "gzip")) {
        uLong compSize = data_len;
        uLongf ucompSize = 4096 * 1024;
        uncompressed_data = malloc(4096 * 1024);

        z_stream zStream;
        memset(&zStream, 0, sizeof(zStream));
        inflateInit2(&zStream, 16);

        zStream.next_in = (Bytef *) data;
        zStream.avail_in = compSize;
        zStream.next_out = (Bytef *) uncompressed_data;
        zStream.avail_out = ucompSize;

        inflate(&zStream, Z_FINISH);
        inflateEnd(&zStream);

        d = uncompressed_data;
    }

    save_json(d);

    json_t *root;
    json_error_t error;
    root = json_loads(d, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    struct timeval st, et;

    res_metadata_t result = {0};
    uint32_t rc;
    pthread_rwlock_rdlock(&data_rwlock);

    gettimeofday(&st, NULL);
    rc = recognize(root, &result);
    gettimeofday(&et, NULL);

    json_decref(root);

    pthread_rwlock_unlock(&data_rwlock);

    uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();

    json_object_set_new(obj, "time", json_integer(elapsed));

    if (*result.type) json_object_set(obj, "type", json_string(result.type));
    json_object_set(obj, "title", json_string(result.title));
    json_object_set(obj, "authors", authors_to_json(result.authors));
    if (*result.doi) json_object_set(obj, "doi", json_string(result.doi));
    if (*result.isbn) json_object_set(obj, "isbn", json_string(result.isbn));
    if (*result.arxiv) json_object_set(obj, "arxiv", json_string(result.arxiv));
    if (*result.abstract) json_object_set(obj, "abstract", json_string(result.abstract));
    if (*result.year) json_object_set(obj, "year", json_string(result.year));
    if (*result.container) json_object_set(obj, "container", json_string(result.container));
    if (*result.publisher) json_object_set(obj, "publisher", json_string(result.publisher));
    if (*result.pages) json_object_set(obj, "pages", json_string(result.pages));
    if (*result.volume) json_object_set(obj, "volume", json_string(result.volume));
    if (*result.issue) json_object_set(obj, "issue", json_string(result.issue));
    if (*result.issn) json_object_set(obj, "issn", json_string(result.issue));
    if (*result.url) json_object_set(obj, "url", json_string(result.url));

    log_debug(
            "\n\nus: %d\ntype: %s\ntitle: %s\nauthors: %s\ndoi: %s\nisbn: %s\narxiv: %s\nyear: %s\ncontainer: %s\npublisher: %s\nabstract: %s\npages: %s\nvolume: %s\nissue: %s\nissn: %s\n",
            elapsed,
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

    if (uncompressed_data) free(uncompressed_data);

    return OCS_PROCESSED;
}

onion_connection_status url_index(void *_, onion_request *req, onion_response *res) {
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
    json_object_set(obj, "total_titles", json_integer(stats.total_titles));

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

int main(int argc, char **argv) {
    char *opt_db_directory = 0;
    char *opt_port = 0;

    int opt;
    while ((opt = getopt(argc, argv, "d:p:i:l:")) != -1) {
        switch (opt) {
            case 'd':
                opt_db_directory = optarg;
                break;
            case 'l':
                if(optarg) {
                    log_level = strtol(optarg, 0, 10);
                }
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

    log_info("starting in normal mode");


    setenv("ONION_LOG", "noinfo", 1);
    pthread_rwlock_init(&data_rwlock, 0);
    pthread_rwlock_init(&saver_rwlock, 0);

    if (!text_init()) {
        log_error("failed to initialize text processor");
        return EXIT_FAILURE;
    }

    journal_init();
    wordlist_init();

    if (!db_normal_mode_init(opt_db_directory)) {
        log_error("failed to initialize db normal mode");
        return EXIT_FAILURE;
    }


    if (!ht_init()) {
        log_error("failed to initialize hashtable");
        return EXIT_FAILURE;
    }

    stats_t stats = ht_stats();
    log_info("\nused_rows=%u\ntotal_titles=%u\n",
             stats.used_rows, stats.total_titles);

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

    if (!indexing_mode) onion_url_add(urls, "recognize", url_recognize);
    onion_url_add(urls, "index", url_index);
    onion_url_add(urls, "stats", url_stats);
    log_info("listening on port %s", opt_port);

    onion_listen(on);

    onion_free(on);
    return EXIT_SUCCESS;
}
