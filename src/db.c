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
#include "ht.h"
#include "db.h"
#include "text.h"
#include "dedup.h"
#include "log.h"

extern uint8_t indexing_mode;

sqlite3 *sqlite_ht;
sqlite3 *sqlite_fields;
sqlite3 *sqlite_fields_read;
sqlite3 *sqlite_doidata;
sqlite3 *sqlite_doidata_read;
sqlite3 *sqlite_thmh;
sqlite3 *sqlite_thmh_read;
sqlite3 *sqlite_fhmh;
sqlite3 *sqlite_fhmh_read;
sqlite3 *sqlite_ahmh;
sqlite3 *sqlite_ahmh_read;

sqlite3_stmt *fields_insert_stmt = 0;
sqlite3_stmt *doidata_insert_stmt = 0;
sqlite3_stmt *thmh_insert_stmt = 0;
sqlite3_stmt *fhmh_insert_stmt = 0;
sqlite3_stmt *ahmh_insert_stmt = 0;

uint32_t fields_in_transaction = 0;
uint32_t doidata_in_transaction = 0;
uint32_t thmh_in_transaction = 0;
uint32_t fhmh_in_transaction = 0;
uint32_t ahmh_in_transaction = 0;

int db_normal_mode_init(char *directory) {
    int rc;
    char *sql;
    char *err_msg;
    char path_ht[PATH_MAX];
    char path_fields[PATH_MAX];
    char path_doidata[PATH_MAX];
    char path_thmh[PATH_MAX];
    char path_fhmh[PATH_MAX];
    char path_ahmh[PATH_MAX];

    snprintf(path_ht, PATH_MAX, "%s/ht.sqlite", directory);
    snprintf(path_fields, PATH_MAX, "%s/fields.sqlite", directory);
    snprintf(path_doidata, PATH_MAX, "%s/doidata.sqlite", directory);
    snprintf(path_thmh, PATH_MAX, "%s/thmh.sqlite", directory);
    snprintf(path_fhmh, PATH_MAX, "%s/fhmh.sqlite", directory);
    snprintf(path_ahmh, PATH_MAX, "%s/ahmh.sqlite", directory);

    if ((rc = sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        log_error("(%i)", rc);
        return 0;
    }

    // ht
    if ((rc = sqlite3_open(path_ht, &sqlite_ht)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ht, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    // fields
    if ((rc = sqlite3_open(path_fields, &sqlite_fields)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fields, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO fields (mh,dh,data) VALUES (?,?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fields, sql, -1, &fields_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_open(path_fields, &sqlite_fields_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fields, rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }



    // doidata
    if ((rc = sqlite3_open(path_doidata, &sqlite_doidata)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_doidata, rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO doidata (mh,data) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_doidata, sql, -1, &doidata_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_open(path_doidata, &sqlite_doidata_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_doidata, rc, sqlite3_errmsg(sqlite_doidata_read));
        return 0;
    }



    // thmh
    if ((rc = sqlite3_open(path_thmh, &sqlite_thmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_thmh, rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_thmh));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO thmh (th,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_thmh, sql, -1, &thmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_open(path_thmh, &sqlite_thmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_thmh, rc, sqlite3_errmsg(sqlite_thmh_read));
        return 0;
    }

    // fhmh
    if ((rc = sqlite3_open(path_fhmh, &sqlite_fhmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fhmh, rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fhmh));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO fhmh (fh,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fhmh, sql, -1, &fhmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_open(path_fhmh, &sqlite_fhmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fhmh, rc, sqlite3_errmsg(sqlite_fhmh_read));
        return 0;
    }

    // ahmh
    if ((rc = sqlite3_open(path_ahmh, &sqlite_ahmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ahmh, rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    sql = "PRAGMA journal_mode = WAL;";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ahmh));
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT OR IGNORE INTO ahmh (ah,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_ahmh, sql, -1, &ahmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_open(path_ahmh, &sqlite_ahmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ahmh, rc, sqlite3_errmsg(sqlite_ahmh_read));
        return 0;
    }

    return 1;
}

int db_indexing_mode_init(char *directory) {
    char *sql;
    int rc;
    char *err_msg;

    // Init file paths
    char path_ht[PATH_MAX];
    char path_fields[PATH_MAX];
    char path_doidata[PATH_MAX];
    char path_thmh[PATH_MAX];
    char path_fhmh[PATH_MAX];
    char path_ahmh[PATH_MAX];

    snprintf(path_ht, PATH_MAX, "%s/ht.sqlite", directory);
    snprintf(path_fields, PATH_MAX, "%s/fields.sqlite", directory);
    snprintf(path_doidata, PATH_MAX, "%s/doidata.sqlite", directory);
    snprintf(path_thmh, PATH_MAX, "%s/thmh.sqlite", directory);
    snprintf(path_fhmh, PATH_MAX, "%s/fhmh.sqlite", directory);
    snprintf(path_ahmh, PATH_MAX, "%s/ahmh.sqlite", directory);

    // Make sure sqlite is in serialized mode
    if ((rc = sqlite3_config(SQLITE_CONFIG_SERIALIZED)) != SQLITE_OK) {
        log_error("(%i)", rc);
        return 0;
    }

    // ht
    if ((rc = sqlite3_open(path_ht, &sqlite_ht)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ht, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_ht, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS ht (id INTEGER, ah_len INTEGER, th_len INTEGER, data BLOB);";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_ht";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // fields
    if ((rc = sqlite3_open(path_fields, &sqlite_fields)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fields, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_fields, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // It's faster to deduplicate data field not on 'data' field but on its hash 'dh'
    sql = "CREATE TABLE IF NOT EXISTS fields (mh INTEGER, dh INTEGER, data BLOB)";
    if ((rc = sqlite3_exec(sqlite_fields, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO fields (mh,dh,data) VALUES (?,?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fields, sql, -1, &fields_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_fields";
    if ((rc = sqlite3_exec(sqlite_fields, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    if ((rc = sqlite3_open(path_fields, &sqlite_fields_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fields, rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }


    // doidata
    if ((rc = sqlite3_open(path_doidata, &sqlite_doidata)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_doidata, rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS doidata (mh INTEGER, data BLOB)";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO doidata (mh,data) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_doidata, sql, -1, &doidata_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_doidata";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    if ((rc = sqlite3_open(path_doidata, &sqlite_doidata_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_doidata, rc, sqlite3_errmsg(sqlite_doidata_read));
        return 0;
    }


    // thmh
    if ((rc = sqlite3_open(path_thmh, &sqlite_thmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_thmh, rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }


    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS thmh (th INTEGER, mh INTEGER)";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO thmh (th,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_thmh, sql, -1, &thmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_thmh";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    if ((rc = sqlite3_open(path_thmh, &sqlite_thmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_thmh, rc, sqlite3_errmsg(sqlite_thmh_read));
        return 0;
    }

    // fhmh
    if ((rc = sqlite3_open(path_fhmh, &sqlite_fhmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fhmh, rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS fhmh (fh INTEGER, mh INTEGER)";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO fhmh (fh,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_fhmh, sql, -1, &fhmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_fhmh";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    if ((rc = sqlite3_open(path_fhmh, &sqlite_fhmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_fhmh, rc, sqlite3_errmsg(sqlite_fhmh_read));
        return 0;
    }

    // ahmh
    if ((rc = sqlite3_open(path_ahmh, &sqlite_ahmh)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ahmh, rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    sql = "PRAGMA journal_mode = DELETE;";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "PRAGMA synchronous = OFF";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, NULL, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "CREATE TABLE IF NOT EXISTS ahmh (ah INTEGER, mh INTEGER)";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "INSERT INTO ahmh (ah,mh) VALUES (?,?);";
    if ((rc = sqlite3_prepare_v2(sqlite_ahmh, sql, -1, &ahmh_insert_stmt, 0)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "DROP INDEX IF EXISTS idx_ahmh";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    if ((rc = sqlite3_open(path_ahmh, &sqlite_ahmh_read)) != SQLITE_OK) {
        log_error("%s (%d): %s", path_ahmh, rc, sqlite3_errmsg(sqlite_ahmh_read));
        return 0;
    }

    db_dedup_reinit_fields();
    db_dedup_reinit_doidata();
    db_dedup_reinit_thmh();
    db_dedup_reinit_ahmh();
    db_dedup_reinit_fhmh();

    return 1;
}

int db_indexing_mode_finish() {
    char *sql;
    int rc;
    char *err_msg;

    sql = "CREATE INDEX idx_ht ON ht (id)";
    if ((rc = sqlite3_exec(sqlite_ht, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // fields
    sql = "CREATE UNIQUE INDEX idx_fields ON fields (mh,dh)";
    if ((rc = sqlite3_exec(sqlite_fields, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // doidata
    sql = "CREATE UNIQUE INDEX idx_doidata ON doidata (mh)";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // thmh
    sql = "CREATE UNIQUE INDEX idx_thmh ON thmh (th,mh)";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // fhmh
    sql = "CREATE UNIQUE INDEX idx_fhmh ON fhmh (fh,mh)";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    // ahmh
    sql = "CREATE UNIQUE INDEX idx_ahmh ON ahmh (ah,mh)";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, 0, 0, &err_msg)) != SQLITE_OK) {
        log_error("%s (%d): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        return 0;
    }

    return 1;
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
    if ((rc = sqlite3_finalize(fields_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fields)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fields_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    // doidata
    if ((rc = sqlite3_finalize(doidata_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_doidata)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_doidata_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_doidata_read));
        return 0;
    }

    // fhmh
    if ((rc = sqlite3_finalize(thmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_thmh)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_thmh_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_thmh_read));
        return 0;
    }

    // fhmh
    if ((rc = sqlite3_finalize(fhmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fhmh)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_fhmh_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fhmh_read));
        return 0;
    }

    // ahmh
    if ((rc = sqlite3_finalize(ahmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ahmh)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_close(sqlite_ahmh_read)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ahmh_read));
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
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields));
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fields, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields));
        sqlite3_free(err_msg);
        //return 0;
    }
    fields_in_transaction = 0;
    return 1;
}

int db_doidata_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata));
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_doidata, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata));
        sqlite3_free(err_msg);
        //return 0;
    }
    doidata_in_transaction = 0;
    return 1;
}

int db_thmh_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_thmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }
    thmh_in_transaction = 0;
    return 1;
}

int db_fhmh_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_fhmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }
    fhmh_in_transaction = 0;
    return 1;
}

int db_ahmh_save() {
    char *sql;
    char *err_msg = 0;
    int rc;

    sql = "END TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }

    sql = "BEGIN TRANSACTION";
    if ((rc = sqlite3_exec(sqlite_ahmh, sql, NULL, NULL, &err_msg)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, err_msg);
        sqlite3_free(err_msg);
        //return 0;
    }
    ahmh_in_transaction = 0;
    return 1;
}

int db_fields_insert(uint64_t mh, uint8_t *data, uint32_t data_len) {
    int rc;

    uint64_t dh = text_hash64(data, data_len) >> 24; // 40 bit

    if (indexing_mode) {
        rc = dedup_fields(mh, dh);
        if (rc == DEDUP_ERROR) return 0;
        if (rc == DEDUP_DUPLICATED) return 1;
    }

    if ((rc = sqlite3_bind_int64(fields_insert_stmt, 1, mh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(fields_insert_stmt, 2, dh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_bind_blob(fields_insert_stmt, 3, data, data_len, SQLITE_STATIC)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_step(fields_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    if ((rc = sqlite3_reset(fields_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    fields_in_transaction++;
    return 1;
}

int db_doidata_insert(uint64_t mh, uint8_t *data, uint32_t data_len) {
    int rc;

    if (indexing_mode) {
        rc = dedup_doidata(mh);
        if (rc == DEDUP_ERROR) return 0;
        if (rc == DEDUP_DUPLICATED) return 1;
    }

    if ((rc = sqlite3_bind_int64(doidata_insert_stmt, 1, mh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_bind_blob(doidata_insert_stmt, 2, data, data_len, SQLITE_STATIC)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_step(doidata_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    if ((rc = sqlite3_reset(doidata_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    doidata_in_transaction++;
    return 1;
}

int db_thmh_insert(uint64_t th, uint64_t mh) {
    int rc;

    if (indexing_mode) {
        rc = dedup_hmh(1, th, mh);
        if (rc == DEDUP_ERROR) return 0;
        if (rc == DEDUP_DUPLICATED) return 1;
    }

    if ((rc = sqlite3_bind_int64(thmh_insert_stmt, 1, th)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(thmh_insert_stmt, 2, mh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_step(thmh_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    if ((rc = sqlite3_reset(thmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    thmh_in_transaction++;
    return 1;
}

int db_fhmh_insert(uint64_t fh, uint64_t th) {
    int rc;

    if (indexing_mode) {
        rc = dedup_hmh(3, fh, th);
        if (rc == DEDUP_ERROR) return 0;
        if (rc == DEDUP_DUPLICATED) return 1;
    }

    if ((rc = sqlite3_bind_int64(fhmh_insert_stmt, 1, fh)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(fhmh_insert_stmt, 2, th)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_step(fhmh_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    if ((rc = sqlite3_reset(fhmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    fhmh_in_transaction++;
    return 1;
}

int db_ahmh_insert(uint64_t ah, uint64_t th) {
    int rc;

    if (indexing_mode) {
        rc = dedup_hmh(2, ah, th);
        if (rc == DEDUP_ERROR) return 0;
        if (rc == DEDUP_DUPLICATED) return 1;
    }

    if ((rc = sqlite3_bind_int64(ahmh_insert_stmt, 1, ah)) != SQLITE_OK) {
        log_error("sqlite3_bind_text: (%i): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(ahmh_insert_stmt, 2, th)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_step(ahmh_insert_stmt)) != SQLITE_DONE) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    if ((rc = sqlite3_reset(ahmh_insert_stmt)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    ahmh_in_transaction++;
    return 1;
}

sqlite3_stmt *db_thmhs(uint64_t th, uint64_t *mhs, uint32_t *mhs_len) {
    uint32_t mhs_max_len = *mhs_len;
    *mhs_len = 0;
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT mh FROM thmh WHERE th = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_thmh_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_thmh_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, th)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_thmh_read));
        return 0;
    }


    while (sqlite3_step(stmt) == SQLITE_ROW && *mhs_len < mhs_max_len) {
        *(mhs + *mhs_len) = sqlite3_column_int64(stmt, 0);
        (*mhs_len)++;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_thmh_read));
    }

    return 1;
}

sqlite3_stmt *db_fhmhs(uint64_t fh, uint64_t *mhs, uint32_t *mhs_len) {
    uint32_t mhs_max_len = *mhs_len;
    *mhs_len = 0;
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT mh FROM fhmh WHERE fh = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_fhmh_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fhmh_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, fh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fhmh_read));
        return 0;
    }


    while (sqlite3_step(stmt) == SQLITE_ROW && *mhs_len < mhs_max_len) {
        *(mhs + *mhs_len) = sqlite3_column_int64(stmt, 0);
        (*mhs_len)++;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fhmh_read));
    }

    return 1;
}

sqlite3_stmt *db_ahmhs(uint64_t ah, uint64_t *mhs, uint32_t *mhs_len) {
    uint32_t mhs_max_len = *mhs_len;
    *mhs_len = 0;
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT mh FROM ahmh WHERE ah = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_ahmh_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ahmh_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, ah)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ahmh_read));
        return 0;
    }


    while (sqlite3_step(stmt) == SQLITE_ROW && *mhs_len < mhs_max_len) {
        *(mhs + *mhs_len) = sqlite3_column_int64(stmt, 0);
        (*mhs_len)++;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ahmh_read));
    }

    return 1;
}

sqlite3_stmt *db_get_fields_stmt(uint64_t th) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT data FROM fields WHERE mh = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_fields_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, th)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_fields_read));
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
        log_error("(%d):", rc);
    }
    return 0;
}

sqlite3_stmt *db_get_doidata_stmt(uint64_t mh) {
    char *sql;
    int rc;

    sqlite3_stmt *stmt = NULL;
    sql = "SELECT data FROM doidata WHERE mh = ?";
    if ((rc = sqlite3_prepare_v2(sqlite_doidata_read, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata_read));
        return 0;
    }

    if ((rc = sqlite3_bind_int64(stmt, 1, mh)) != SQLITE_OK) {
        log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_doidata_read));
        return 0;
    }

    return stmt;
}

int db_get_next_doidata(sqlite3_stmt *stmt, uint8_t **data, uint32_t *data_len) {
    int rc;

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        *data = sqlite3_column_blob(stmt, 0);
        *data_len = sqlite3_column_bytes(stmt, 0);
        return 1;
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d):", rc);
    }
    return 0;
}

int db_ht_save(row_t *rows, uint32_t rows_len) {
    char *sql;
    char *err_msg;
    int rc;

    sql = "INSERT OR REPLACE INTO ht (id, ah_len, th_len, data) VALUES (?,?,?,?);";
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

        if ((rc = sqlite3_bind_int(stmt, 2, row->ah_len)) != SQLITE_OK) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }

        if ((rc = sqlite3_bind_int(stmt, 3, row->th_len)) != SQLITE_OK) {
            log_error("(%i): %s", rc, sqlite3_errmsg(sqlite_ht));
            return 0;
        }

        if ((rc = sqlite3_bind_blob(stmt, 4, row->slots, sizeof(slot_t) * (row->ah_len + row->th_len),
                                    SQLITE_STATIC)) != SQLITE_OK) {
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

    sql = "SELECT id, ah_len, th_len, data FROM ht";
    if ((rc = sqlite3_prepare_v2(sqlite_ht, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ht));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint32_t id = (uint32_t) sqlite3_column_int(stmt, 0);
        uint32_t ah_len = (uint32_t) sqlite3_column_int(stmt, 1);
        uint32_t th_len = (uint32_t) sqlite3_column_int(stmt, 2);
        uint8_t *data = sqlite3_column_blob(stmt, 3);
        uint32_t len = (uint32_t) sqlite3_column_bytes(stmt, 3);

        if (ah_len + th_len != len / sizeof(slot_t)) {
            log_error("db_load_ht: invalid slots len");
            return 0;
        }

        if (id >= HASHTABLE_SIZE) {
            log_error("db_load_ht: too high rowid");
            return 0;
        }

        rows[id].ah_len = ah_len;
        rows[id].th_len = th_len;
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

int db_dedup_reinit_fields() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT mh, dh FROM fields";
    if ((rc = sqlite3_prepare_v2(sqlite_fields, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t mh = (uint64_t) sqlite3_column_int64(stmt, 0);
        uint64_t dh = (uint64_t) sqlite3_column_int64(stmt, 1);
        rc = dedup_fields(mh, dh);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fields));
        return 0;
    }
}

int db_dedup_reinit_doidata() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT mh FROM doidata";
    if ((rc = sqlite3_prepare_v2(sqlite_doidata, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t mh = (uint64_t) sqlite3_column_int64(stmt, 0);
        rc = dedup_doidata(mh);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_doidata));
        return 0;
    }
}

int db_dedup_reinit_thmh() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT th, mh FROM thmh";
    if ((rc = sqlite3_prepare_v2(sqlite_thmh, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t th = (uint64_t) sqlite3_column_int64(stmt, 0);
        uint64_t mh = (uint64_t) sqlite3_column_int64(stmt, 1);
        rc = dedup_hmh(1, th, mh);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_thmh));
        return 0;
    }
}

int db_dedup_reinit_ahmh() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT ah, mh FROM ahmh";
    if ((rc = sqlite3_prepare_v2(sqlite_ahmh, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t ah = (uint64_t) sqlite3_column_int64(stmt, 0);
        uint64_t mh = (uint64_t) sqlite3_column_int64(stmt, 1);
        rc = dedup_hmh(2, ah, mh);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_ahmh));
        return 0;
    }
}

int db_dedup_reinit_fhmh() {
    int rc;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    sql = "SELECT fh, mh FROM fhmh";
    if ((rc = sqlite3_prepare_v2(sqlite_fhmh, sql, -1, &stmt, NULL)) != SQLITE_OK) {
        log_error("%s (%i): %s", sql, rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        uint64_t fh = (uint64_t) sqlite3_column_int64(stmt, 0);
        uint64_t mh = (uint64_t) sqlite3_column_int64(stmt, 1);
        rc = dedup_hmh(3, fh, mh);
    }

    if ((rc = sqlite3_finalize(stmt)) != SQLITE_OK) {
        log_error("(%d): %s", rc, sqlite3_errmsg(sqlite_fhmh));
        return 0;
    }
}

uint32_t db_fields_in_transaction() {
    return fields_in_transaction;
}

uint32_t db_fhmh_in_transaction() {
    return fhmh_in_transaction;
}

uint32_t db_ahmh_in_transaction() {
    return ahmh_in_transaction;
}