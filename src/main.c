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
#include "dedup.h"
#include "log.h"

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

json_t *get_identifiers_json(result_t *result) {
    json_t *json_identifiers = json_array();

    for (uint32_t i = 0; i < result->metadata.identifiers_len; i++) {
        json_array_append(json_identifiers, json_string(result->metadata.identifiers[i]));
    }
    return json_identifiers;
}

onion_connection_status url_recognize(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);

    if (!dreq) return OCS_PROCESSED;

    const char *data = onion_block_data(dreq);

    json_t *root;
    json_error_t error;
    root = json_loads(data, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    json_t *json_hash = json_object_get(root, "hash");
    json_t *json_text = json_object_get(root, "text");

    if (!json_is_string(json_hash) && !json_is_string(json_text)) {
        return OCS_PROCESSED;;
    }

    uint8_t *hash = json_string_value(json_hash);
    uint8_t *text = json_string_value(json_text);

    struct timeval st, et;

    result_t result = {0};
    uint32_t rc;
    pthread_rwlock_rdlock(&data_rwlock);

    gettimeofday(&st, NULL);
    rc = recognize(hash, text, &result);
    gettimeofday(&et, NULL);

    pthread_rwlock_unlock(&data_rwlock);

    uint32_t elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();

    json_object_set_new(obj, "time", json_integer(elapsed));
    if (rc) {
        json_object_set(obj, "title", json_string(result.metadata.title));
        json_object_set(obj, "authors", authors_to_json(result.metadata.authors));
        json_object_set(obj, "abstract", json_string(result.metadata.abstract));
        json_object_set(obj, "year", json_integer(result.metadata.year));
        json_object_set(obj, "identifiers", get_identifiers_json(&result));
    }

    json_object_set(obj, "detected_titles", json_integer(result.detected_titles));
    json_object_set(obj, "detected_abstracts", json_integer(result.detected_abstracts));
    json_object_set(obj, "detected_metadata_through_title", json_integer(result.detected_metadata_through_title));
    json_object_set(obj, "detected_metadata_through_abstract", json_integer(result.detected_metadata_through_abstract));
    json_object_set(obj, "detected_metadata_through_hash", json_integer(result.detected_metadata_through_hash));


    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, str);
    free(str);

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
                    json_t *json_abstract = json_object_get(el, "abstract");
                    json_t *json_year = json_object_get(el, "year");
                    json_t *json_identifiers = json_object_get(el, "identifiers");
                    json_t *json_hash = json_object_get(el, "hash");

                    metadata_t metadata;
                    memset(&metadata, 0, sizeof(metadata_t));

                    if (json_is_string(json_title)) metadata.title = json_string_value(json_title);
                    if (json_is_string(json_authors)) metadata.authors = json_string_value(json_authors);
                    if (json_is_string(json_abstract)) metadata.abstract = json_string_value(json_abstract);
                    if (json_is_string(json_year)) metadata.year = json_string_value(json_year);
                    if (json_is_string(json_identifiers)) metadata.identifiers = json_string_value(json_identifiers);
                    if (json_is_string(json_hash)) metadata.hash = json_string_value(json_hash);

                    if (index_metadata(&metadata)) indexed++;
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
        onion_response_printf(res, str);
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
    onion_response_printf(res, str);
    free(str);

    return OCS_PROCESSED;
}

int save() {
    pthread_rwlock_wrlock(&saver_rwlock);
    pthread_rwlock_rdlock(&data_rwlock);
    log_info("saving");
    db_fields_save();
    db_thmh_save();
    db_fhmh_save();
    db_ahmh_save();
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

            if (db_fields_in_transaction() >= 50000000) {
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

    if (indexing_mode) {
        log_info("finalizing indexing mode");
        ht_save();
        db_indexing_mode_finish();
        log_info("finished");
    } else {
        log_info("saving");
        ht_save();
        db_fields_save();
        db_thmh_save();
        db_fhmh_save();
        db_ahmh_save();
        log_info("saved");
    }

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
            "-i\tstart in indexing mode (TEMPORARY DISABLED)\n" \
            "Usage example:\n" \
            "recognizer-server -d /var/db -p 8080\n"
    );
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
//            case 'i':
//                indexing_mode = 1;
//                break;
            default:
                print_usage();
                return EXIT_FAILURE;
        }
    }

    if (!opt_db_directory || !opt_port) {
        print_usage();
        return EXIT_FAILURE;
    }

    DIR *dir = opendir(opt_db_directory);
    if (!dir) {
        log_error("database directory is invalid");
        return EXIT_FAILURE;
    }

    indexing_mode = 1;

    uint32_t n = 0;
    while (readdir(dir)) {
        n++;
        if (n == 3) {
            indexing_mode = 0;
            break;
        }
    }
    closedir(dir);

    if (indexing_mode) {
        if (!dedup_init()) {
            log_error("failed to initialize deduplicator");
            return EXIT_FAILURE;
        }
        log_info("starting in indexing mode");
    } else {
        log_info("starting in normal mode");
    }

    setenv("ONION_LOG", "noinfo", 1);
    pthread_rwlock_init(&data_rwlock, 0);
    pthread_rwlock_init(&saver_rwlock, 0);

    if (!text_init()) {
        log_error("failed to initialize text processor");
        return EXIT_FAILURE;
    }

    if (indexing_mode) {
        if (!db_indexing_mode_init(opt_db_directory)) {
            log_error("failed to initialize db indexing mode");
            return EXIT_FAILURE;
        }
    } else {
        if (!db_normal_mode_init(opt_db_directory)) {
            log_error("failed to initialize db normal mode");
            return EXIT_FAILURE;
        }
    }

    if (!ht_init()) {
        log_error("failed to initialize hashtable");
        return EXIT_FAILURE;
    }

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

    onion_url_add(urls, "recognize", url_recognize);
    onion_url_add(urls, "index", url_index);
    onion_url_add(urls, "stats", url_stats);
    onion_url_add_handler(urls, "panel", onion_handler_export_local_new("static/panel.html"));

    log_info("listening on port %s", opt_port);

    onion_listen(on);

    onion_free(on);
    return EXIT_SUCCESS;
}