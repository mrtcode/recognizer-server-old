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
#include <linux/limits.h>
#include "ht.h"
#include "db.h"
#include "text.h"

sqlite3 *sqlite_hashtable;
sqlite3 *sqlite_fields;
sqlite3 *sqlite_fields_read;
sqlite3 *sqlite_fhth;
sqlite3 *sqlite_fhth_read;
sqlite3 *sqlite_ahth;
sqlite3 *sqlite_ahth_read;

sqlite3_stmt *fields_insert_stmt = 0;
sqlite3_stmt *fhth_insert_stmt = 0;
sqlite3_stmt *ahth_insert_stmt = 0;

uint32_t fields_in_transaction = 0;
uint32_t fhth_in_transaction = 0;
uint32_t ahth_in_transaction = 0;

int db_init(char *directory) {
    int rc;
    char path_hashtable[PATH_MAX];
    char path_identifiers[PATH_MAX];
    char path_fhth[PATH_MAX];
    char path_ahth[PATH_MAX];

    snprintf(path_hashtable, PATH_MAX, "%s/ht.sqlite", directory);
    snprintf(path_identifiers, PATH_MAX, "%s/fields.sqlite", directory);
    snprintf(path_fhth, PATH_MAX, "%s/fhth.sqlite", directory);
    snprintf(path_ahth, PATH_MAX, "%s/ahth.sqlite", directory);

    if ((rc = sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_config: (%i)\n", rc);
        return 0;
    }

    if (!db_hashtable_init(path_hashtable)) {
        return 0;
    }

    if (!db_fields_init(path_identifiers)) {
        return 0;
    }

    if (!db_fhth_init(path_fhth)) {
        return 0;
    }

    if (!db_ahth_init(path_ahth)) {
        return 0;
    }

    return 1;
}

int db_close() {
    int rc;
    printf("closing db\n");

    // hashtable
    if ((rc = sqlite3_close(sqlite_hashtable)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    // fields
    if ((rc = sqlite3_finalize(fields_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fields)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fields_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    // fhth
    if ((rc = sqlite3_finalize(fhth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fhth)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fhth_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fhth_read));
        return 0;
    }

    // ahth
    if ((rc = sqlite3_finalize(ahth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ahth)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ahth_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_close: (%d): %s\n", rc, sqlite3_errmsg(sqlite_ahth_read));
        return 0;
    }


    return 1;
}

int db_hashtable_init(char *path) {
    char *sql;
    int rc;
    char *err_msg;
    if ((rc = sqlite3_open(path, &sqlite_hashtable)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS hashtable (id INTEGER PRIMARY KEY, ah_len INTEGER, th_len INTEGER, data BLOB);";
    if ((rc = sqlite3_exec(sqlite_hashtable, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    return 1;
}

int db_fields_init(char *path) {
    char *sql;
    int rc;
    char *err_msg = 0;

    if ((rc = sqlite3_open(path, &sqlite_fields)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    sql = "PRAGMA cache_size = 200000;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS fields (hash INTEGER, sum INTEGER, data BLOB, PRIMARY KEY (hash, sum))";
    if ((rc = sqlite3_exec(sqlite_fields, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fields));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO fields (hash,sum,data) VALUES (?,?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fields, sql, -1, &fields_insert_stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_open(path, &sqlite_fields_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    return 1;
}

int db_fhth_init(char *path) {
    char *sql;
    int rc;
    char *err_msg = 0;

    if ((rc = sqlite3_open(path, &sqlite_fhth)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    sql = "PRAGMA cache_size = 100000;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_fhth, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS fhth (fh INTEGER, th INTEGER, PRIMARY KEY (fh, th))";
    if ((rc = sqlite3_exec(sqlite_fhth, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fhth));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO fhth (fh,th) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fhth, sql, -1, &fhth_insert_stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_open(path, &sqlite_fhth_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_fhth_read));
        return 0;
    }

    return 1;
}


int db_ahth_init(char *path) {
    char *sql;
    int rc;
    char *err_msg = 0;

    if ((rc = sqlite3_open(path, &sqlite_ahth)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    sql = "PRAGMA cache_size = 100000;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_ahth, sql, NULL, 0, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS ahth (ah INTEGER, th INTEGER, PRIMARY KEY (ah, th))";
    if ((rc = sqlite3_exec(sqlite_ahth, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%d): %s\n", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ahth));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO ahth (ah,th) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_ahth, sql, -1, &ahth_insert_stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_open(path, &sqlite_ahth_read)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open: %s (%d): %s\n", path, rc, sqlite3_errmsg(sqlite_ahth_read));
        return 0;
    }

    return 1;
}


int db_fields_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fields));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fields));
        sqlite3_free(err_msg);
        return 0;
    }
    fields_in_transaction = 0;
    return 1;
}

int db_fhth_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fhth));
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fhth));
        sqlite3_free(err_msg);
        //return 0;
    }
    fhth_in_transaction = 0;
    return 1;
}

int db_ahth_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ahth));
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahth, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ahth));
        sqlite3_free(err_msg);
        //return 0;
    }
    ahth_in_transaction = 0;
    return 1;
}

int db_fields_insert(uint64_t hash, uint8_t *data, uint32_t data_len) {
    int rc;

    if ((rc = sqlite3_bind_int64(fields_insert_stmt, 1, hash)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(fields_insert_stmt, 2, text_hash64(data, data_len))) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_bind_blob(fields_insert_stmt, 3, data, data_len, SQLITE_STATIC)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_text: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }


    if ((rc = sqlite3_step(fields_insert_stmt)) != SQLITE_DONE) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(fields_insert_stmt));
        return 0;
    }

    if ((rc = sqlite3_clear_bindings(fields_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(fields_insert_stmt));
        return 0;
    }

    if ((rc = sqlite3_reset(fields_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(fields_insert_stmt));
        return 0;
    }

    fields_in_transaction++;
    return 1;
}

int db_fhth_insert(uint64_t fh, uint64_t th) {
    int rc;

    if ((rc = sqlite3_bind_int64(fhth_insert_stmt, 1, fh)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_text: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(fhth_insert_stmt, 2, th)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_step(fhth_insert_stmt)) != SQLITE_DONE) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_clear_bindings(fhth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    if ((rc = sqlite3_reset(fhth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth));
        return 0;
    }

    fhth_in_transaction++;
    return 1;
}

int db_ahth_insert(uint64_t ah, uint64_t th) {
    int rc;

    if ((rc = sqlite3_bind_int64(ahth_insert_stmt, 1, ah)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_text: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(ahth_insert_stmt, 2, th)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_step(ahth_insert_stmt)) != SQLITE_DONE) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_clear_bindings(ahth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    if ((rc = sqlite3_reset(ahth_insert_stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth));
        return 0;
    }

    ahth_in_transaction++;
    return 1;
}

sqlite3_stmt *db_fhth_get_stmt(uint64_t fh) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT th FROM fhth WHERE fh = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_fhth_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fhth_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, fh)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fhth_read));
        return 0;
    }

    return stmt;
}

uint64_t db_fhth_get_next_th(sqlite3_stmt *stmt) {
    char *sql;
    int rc;

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        return sqlite3_column_int64(stmt, 0);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_fhth_read));
    }

    return 0;
}

sqlite3_stmt *db_ahth_get_stmt(uint64_t ah) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT th FROM ahth WHERE ah = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_ahth_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_ahth_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, ah)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_ahth_read));
        return 0;
    }

    return stmt;
}

uint64_t db_ahth_get_next_th(sqlite3_stmt *stmt) {
    char *sql;
    int rc;

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        return sqlite3_column_int64(stmt, 0);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_ahth_read));
    }

    return 0;
}

sqlite3_stmt *db_get_fields_stmt(uint64_t hash) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT data FROM fields WHERE hash = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_fields_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, hash)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_bind_int64: (%i): %s\n", rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    return stmt;
}

int db_get_next_field(sqlite3_stmt *stmt, uint8_t **data, uint32_t *data_len) {
    int rc;

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *data = sqlite3_column_blob(stmt, 0);
        *data_len = sqlite3_column_bytes(stmt, 0);
        return 1;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d):\n", rc);
    }
    return 0;
}

int db_ht_save(row_t *rows, uint32_t rows_len) {
    char *sql;
    char *err_msg;
    int rc;

    sql = "INSERT OR REPLACE INTO hashtable (id, ah_len, th_len, data) VALUES (?,?,?,?);";
    sqlite3_stmt *stmt = NULL;
    if ((rc = sqlite3_prepare_v2(sqlite_hashtable, sql, -1, &stmt, 0)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if (sqlite3_exec(sqlite_hashtable, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_hashtable));
        sqlite3_free(err_msg);
        return 0;
    }

    for (int i = 0; i < rows_len; i++) {
        row_t *row = &rows[i];

        if (!row->updated) continue;
        row->updated = 0;

        if ((rc = sqlite3_bind_int(stmt, 1, i)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_bind_int(stmt, 2, row->ah_len)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_bind_int(stmt, 3, row->th_len)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_int: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_bind_blob(stmt, 4, row->slots, sizeof(slot_t) * (row->ah_len + row->th_len),
                                    SQLITE_STATIC)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_bind_blob: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
            fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_clear_bindings(stmt)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_clear_bindings: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }

        if ((rc = sqlite3_reset(stmt)) != SQLITE_OK) {
            fprintf(stderr, "sqlite3_reset: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
            return 0;
        }
    }

    sql = "END TRANSACTION";
    if (sqlite3_exec(sqlite_hashtable, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    return 1;
}

int db_ht_load(row_t *rows) {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT id, ah_len, th_len, data FROM hashtable";
    if ((rc = sqlite3_prepare_v2(sqlite_hashtable, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_prepare_v2: %s (%i): %s\n", sql, rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint32_t id = (uint32_t) sqlite3_column_int(stmt, 0);
        uint32_t ah_len = (uint32_t) sqlite3_column_int(stmt, 1);
        uint32_t th_len = (uint32_t) sqlite3_column_int(stmt, 2);
        uint8_t *data = sqlite3_column_blob(stmt, 3);
        uint32_t len = (uint32_t) sqlite3_column_bytes(stmt, 3);

        if (ah_len + th_len != len / sizeof(slot_t)) {
            fprintf(stderr, "db_load_hashtable: invalid slots len");
            return 0;
        }

        if (id >= HASHTABLE_SIZE) {
            fprintf(stderr, "db_load_hashtable: too high rowid");
            return 0;
        }

        rows[id].ah_len = ah_len;
        rows[id].th_len = th_len;
        rows[id].slots = malloc(len);
        memcpy(rows[id].slots, data, len);
    }

    if (SQLITE_DONE != rc) {
        fprintf(stderr, "sqlite3_step: (%i): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_finalize: (%d): %s\n", rc, sqlite3_errmsg(sqlite_hashtable));
        return 0;
    }

    return 1;
}

uint32_t db_fields_in_transaction() {
    return fields_in_transaction;
}

uint32_t db_fhth_in_transaction() {
    return fhth_in_transaction;
}

uint32_t db_ahth_in_transaction() {
    return ahth_in_transaction;
}