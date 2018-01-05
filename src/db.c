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

#include <sqlite3.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include "defines.h"
#include "log.h"
#include "ht.h"
#include "db.h"
#include "text.h"


extern uint8_t indexing_mode;

sqlite3 *sqlite_ht;
sqlite3 *sqlite_dois;
sqlite3 *sqlite_dois_read;

sqlite3_stmt *dois_insert_stmt = 0;


uint32_t dois_in_transaction = 0;

int db_normal_mode_init(char *directory) {
    int rc;
    char *sql;
    char *err_msg;
    char path_ht[PATH_MAX];
    char path_dois[PATH_MAX];

    snprintf(path_ht, PATH_MAX, "%s/ht.sqlite", directory);
    snprintf(path_dois, PATH_MAX, "%s/dois.sqlite", directory);

    if ((rc = sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        log_error("(%i)", rc);
        return 0;
    }

    // ht
    if ((rc = sqlite3_open(path_ht, &sqlite_ht)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ht, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS ht (id INTEGER, data BLOB);";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE INDEX IF NOT EXISTS idx_ht ON ht (id)";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // fields
    if ((rc = sqlite3_open(path_dois, &sqlite_dois)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_dois, rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS dois (id INTEGER, doi TEXT)";
    if ((rc = sqlite3_exec(sqlite_dois, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_dois ON dois (id)";
    if ((rc = sqlite3_exec(sqlite_dois, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_dois, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_dois, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO dois (id, doi) VALUES (?,?)";
    if ((rc = sqlite3_prepare_v2(sqlite_dois, sql, -1, &dois_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_open(path_dois, &sqlite_dois_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_dois, rc, sqlite3_errmsg(sqlite_dois_read));
        return 0;
    }

    return 1;
}

uint32_t db_dois_id_last() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT MAX(id) FROM dois";
    if ((rc = sqlite3_prepare_v2(sqlite_dois, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    uint32_t doi_id = 0;

    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        doi_id =  sqlite3_column_int(stmt, 0);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    return doi_id;
}

uint8_t db_get_doi(uint32_t doi_id, uint8_t *doi) {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT doi FROM dois WHERE id = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_dois, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_bind_int(stmt, 1, doi_id)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    int ret = 0;

    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint8_t *str = sqlite3_column_text(stmt, 0);
        if(strlen(str)<=DOI_LEN) {
            strcpy(doi, str);
            ret = 1;
        }
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    return ret;
}

int db_close() {
    int rc;
    log_info("closing db");

    // ht
    if ((rc = sqlite3_close(sqlite_ht)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    // fields
    if ((rc = sqlite3_finalize(dois_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_dois)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_dois_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_dois_read));
        return 0;
    }

    return 1;
}

int db_dois_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_dois, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_dois));
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_dois, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_dois));
        sqlite3_free(err_msg);
        //return 0;
    }
    dois_in_transaction = 0;
    return 1;
}

int db_dois_insert(uint32_t doi_id, uint8_t *doi, uint32_t doi_len) {
    int rc;

    if ((rc = sqlite3_bind_int(dois_insert_stmt, 1, doi_id)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_bind_text(dois_insert_stmt, 2, doi, doi_len, SQLITE_STATIC)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_step(dois_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    if ((rc = sqlite3_reset(dois_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_dois));
        return 0;
    }

    dois_in_transaction++;
    return 1;
}

int db_ht_save(row_t *rows, uint32_t rows_len) {
    char *sql;
    char *err_msg;
    int rc;

    sql = "INSERT OR REPLACE INTO ht (id, data) VALUES (?,?);";
    sqlite3_stmt *stmt = NULL;
    if ((rc = sqlite3_prepare_v2(sqlite_ht, sql, -1, &stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if (sqlite3_exec(sqlite_ht, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ht));
        sqlite3_free(err_msg);
        return 0;
    }

    for (int i = 0; i < rows_len; i++) {
        row_t *row = &rows[i];

        if (!row->updated) continue;
        row->updated = 0;

        if ((rc = sqlite3_bind_int(stmt, 1, i)) != SQLITE_OK) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }

        if ((rc = sqlite3_bind_blob(stmt, 2, row->slots, sizeof(slot_t) * (row->len), SQLITE_STATIC)) != SQLITE_OK) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }

        if ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }

        if ((rc = sqlite3_reset(stmt)) != SQLITE_OK) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }
    }

    sql = "END TRANSACTION";
    if (sqlite3_exec(sqlite_ht, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    return 1;
}

int db_ht_load(row_t *rows) {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT id, data FROM ht";
    if ((rc = sqlite3_prepare_v2(sqlite_ht, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint32_t id = (uint32_t) sqlite3_column_int(stmt, 0);
        uint8_t *data = sqlite3_column_blob(stmt, 1);
        uint32_t len = (uint32_t) sqlite3_column_bytes(stmt, 1);

        if (id >= HASHTABLE_SIZE) {
            log_error("db_load_ht: too high rowid");
            return 0;
        }

        rows[id].len = len/sizeof(slot_t);
        rows[id].slots = malloc(len);
        memcpy(rows[id].slots, data, len);
    }

    if (SQLITE_DONE != rc) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    return 1;
}

uint32_t db_dois_in_transaction() {
    return dois_in_transaction;
}
