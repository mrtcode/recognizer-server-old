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
#include "doidata.h"
#include "text.h"
#include "recognize.h"
#include "log.h"
#include "word.h"
#include "journal.h"

int log_level = 1;
onion *on = NULL;

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

onion_connection_status url_recognize(void *_, onion_request *req, onion_response *res) {
    if (!(onion_request_get_flags(req) & OR_POST)) {
        return OCS_PROCESSED;
    }

    const onion_block *dreq = onion_request_get_data(req);
    if (!dreq) return OCS_PROCESSED;

    const char *content_encoding = onion_request_get_header(req, "Content-Encoding");

    const char *data = onion_block_data(dreq);
    uint32_t data_len = onion_block_size(dreq);

    char *d = data;

    char *uncompressed_data = 0;

    if (content_encoding && !strcmp(content_encoding, "gzip")) {
        uLong compSize = data_len;
        uLongf ucompSize = 4096 * 1024;
        uncompressed_data = calloc(4096, 1024);

        z_stream zStream;
        memset(&zStream, 0, sizeof(zStream));
        inflateInit2(&zStream, 16);

        zStream.next_in = (Bytef *) data;
        zStream.avail_in = compSize;
        zStream.next_out = (Bytef *) uncompressed_data;
        zStream.avail_out = ucompSize - 1;

        int r;
        r = inflate(&zStream, Z_FINISH);
        if (r != Z_OK && r != Z_STREAM_END) {
            free(uncompressed_data);
            return OCS_PROCESSED;
        }

        r = inflateEnd(&zStream);
        if (r != Z_OK) {
            free(uncompressed_data);
            return OCS_PROCESSED;
        }

        d = uncompressed_data;
    }

    json_t *root;
    json_error_t error;
    root = json_loads(d, 0, &error);

    if (!root || !json_is_object(root)) {
        return OCS_PROCESSED;
    }

    struct timeval st, et;

    res_metadata_t result = {0};
    uint32_t rc;

    gettimeofday(&st, NULL);
    rc = recognize(root, &result);
    gettimeofday(&et, NULL);

    json_decref(root);

    uint32_t us = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec);

    json_t *obj = json_object();

    json_object_set_new(obj, "time", json_integer(us));

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

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    log_debug("\n%s",str);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, "%s", str);
    free(str);

    if (uncompressed_data) free(uncompressed_data);

    return OCS_PROCESSED;
}

onion_connection_status url_stats(void *_, onion_request *req, onion_response *res) {
    json_t *obj = json_object();

    char *str = json_dumps(obj, JSON_INDENT(1) | JSON_PRESERVE_ORDER);
    json_decref(obj);

    onion_response_set_header(res, "Content-Type", "application/json; charset=utf-8");
    onion_response_printf(res, "%s", str);
    free(str);

    return OCS_PROCESSED;
}

void signal_handler(int signum) {
    log_info("signal received (%d), shutting down..", signum);

    if (on) {
        onion_listen_stop(on);
    }

    if (!doidata_close()) {
        log_error("doidata close failed");
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
            "-l\tlog level\n" \
            "Usage example:\n" \
            "recognizer-server -d /var/db -p 8080\n"
    );
}

int main(int argc, char **argv) {
    char *opt_db_directory = 0;
    char *opt_port = 0;

    int opt;
    while ((opt = getopt(argc, argv, "d:p:l:")) != -1) {
        switch (opt) {
            case 'd':
                opt_db_directory = optarg;
                break;
            case 'l':
                if (optarg) {
                    log_level = strtol(optarg, 0, 10);
                }
                break;
            case 'p':
                opt_port = optarg;
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

    if (log_level > 0) {
        setenv("ONION_LOG", "noinfo", 1);
    }

    if (!text_init()) {
        log_error("failed to initialize text processor");
        return EXIT_FAILURE;
    }

    log_info("initializing journals");
    if (!journal_init(opt_db_directory)) {
        log_error("failed to initialize journal data");
        return EXIT_FAILURE;
    }

    log_info("initializing words");
    if (!word_init(opt_db_directory)) {
        log_error("failed to initialize word data");
        return EXIT_FAILURE;
    }

    log_info("initializing doidata");
    if (!doidata_init(opt_db_directory)) {
        log_error("failed to initialize doidata");
        return EXIT_FAILURE;
    }

    on = onion_new(O_POOL);

    // Signal handler must be initialized after onion_new
    struct sigaction action;
    memset(&action, 0, sizeof(struct sigaction));
    action.sa_handler = signal_handler;
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    onion_set_port(on, opt_port);
    onion_set_max_threads(on, 16);
    onion_set_max_post_size(on, 5*1024*1024);

    onion_url *urls = onion_root_url(on);

    onion_url_add(urls, "recognize", url_recognize);
    onion_url_add(urls, "stats", url_stats);
    log_info("listening on port %s", opt_port);

    onion_listen(on);

    onion_free(on);
    return EXIT_SUCCESS;
}
