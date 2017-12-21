#ifndef RECOGNIZER_SERVER_DB_H
#define RECOGNIZER_SERVER_DB_H

typedef struct shard {
    sqlite3 *sqlite;
    sqlite3 *sqlite_read;
    sqlite3_stmt *stmt_insert;
} shard_t;

typedef struct iterator {
    sqlite3_stmt *stmt1;
    sqlite3_stmt *stmt2;
} iterator_t;

int db_normal_mode_init(char *directory);

uint32_t db_dois_id_last();

uint32_t db_get_doi(uint32_t doi_id, uint8_t *doi);

int db_close();

int db_dois_insert(uint32_t doi_id, uint8_t *doi, uint32_t doi_len);
int db_dois_save();

int db_ht_save(row_t *rows, uint32_t rows_len);

int db_ht_load(row_t *rows);

uint32_t db_dois_in_transaction();

#endif //RECOGNIZER_SERVER_DB_H
