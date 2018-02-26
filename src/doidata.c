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

#include <sqlite3.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <pthread.h>
#include "defines.h"
#include "log.h"
#include "doidata.h"

pthread_rwlock_t doidata_rwlock;
sqlite3 *doidata_sqlite;
sqlite3_stmt *doidata_stmt = NULL;
sqlite3_stmt *doidata_has_doi_stmt = NULL;

uint32_t doidata_init(char *directory) {
    int rc;
    char *sql;
    char *err_msg;
    char path_dois[PATH_MAX];

    snprintf(path_dois, PATH_MAX, "%s/doidata.sqlite", directory);

    if ((rc = sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        log_error("(%i)", rc);
        return 0;
    }

    // doi
    if ((rc = sqlite3_open(path_dois, &doidata_sqlite)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_dois, rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    sql = "SELECT * FROM doidata WHERE title_hash = ? LIMIT 11";
    if ((rc = sqlite3_prepare_v2(doidata_sqlite, sql, -1, &doidata_stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    sql = "SELECT 1 FROM doidata WHERE doi = ? LIMIT 1";
    if ((rc = sqlite3_prepare_v2(doidata_sqlite, sql, -1, &doidata_has_doi_stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    return 1;
}

uint32_t doidata_get(uint64_t title_hash, doidata_t *doidatas, uint32_t *doidatas_len) {
    int rc;
    char *sql;

    pthread_rwlock_wrlock(&doidata_rwlock);
    if ((rc = sqlite3_bind_int64(doidata_stmt, 1, title_hash)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    uint32_t ret = 0;

    *doidatas_len = 0;

    while ((rc = sqlite3_step(doidata_stmt)) == SQLITE_ROW) {
        doidata_t *doi = &doidatas[(*doidatas_len)++];

        if (*doidatas_len == 6) {
            ret = 0;
            break;
        }

        doi->author1_len = sqlite3_column_int(doidata_stmt, 1);
        doi->author1_hash = sqlite3_column_int(doidata_stmt, 2);
        doi->author2_len = sqlite3_column_int(doidata_stmt, 3);
        doi->author2_hash = sqlite3_column_int(doidata_stmt, 4);

        uint8_t *str = sqlite3_column_text(doidata_stmt, 5);
        if (strlen(str) <= DOI_LEN) {
            strcpy(doi->doi, str);
            ret = 1;
        }
    }

    if ((rc = sqlite3_reset(doidata_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    pthread_rwlock_unlock(&doidata_rwlock);

    return ret;
}

uint32_t doidata_has_doi(uint8_t *doi) {
    int rc;
    uint32_t ret = 0;

    pthread_rwlock_wrlock(&doidata_rwlock);
    if ((rc = sqlite3_bind_text(doidata_has_doi_stmt, 1, doi, strlen(doi), SQLITE_STATIC)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    if ((rc = sqlite3_step(doidata_has_doi_stmt)) == SQLITE_ROW) {
        ret = 1;
    }

    if ((rc = sqlite3_reset(doidata_has_doi_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    pthread_rwlock_unlock(&doidata_rwlock);

    return ret;
}

uint32_t doidata_close() {
    int rc;
    log_info("closing db");

    // doi
    if ((rc = sqlite3_finalize(doidata_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d):\n", rc);
    }

    if ((rc = sqlite3_close(doidata_sqlite)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(doidata_sqlite));
        return 0;
    }

    return 1;
}
